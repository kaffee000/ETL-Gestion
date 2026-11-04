# Archivo: dags/etl_cne_indicadores.py
"""
ETL CNE Chile, indicadores de energía y relación con los indicadores CMF.

Flujo visible en Airflow:
  verificar_login -> limpiar_staging -> extract_data[] -> transform_data[] -> load_to_postgres
      -> (kpi_estaciones | kpi_generacion_anual | kpi_mercado_anual)
      -> consolidar_indicadores -> cruzar_con_cmf

Requiere CNE_EMAIL y CNE_PASSWORD en .env. La tabla cmf_indicadores_gestion la genera
el DAG etl_cmf_indicadores, que debe haberse ejecutado al menos una vez.
"""
from datetime import datetime, timedelta
import json
import os
import shutil
import time

import pandas as pd
import requests
import sqlalchemy

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from cne_cliente import ClienteCNE, CNEAuthError, CNERespuestaError
from endpoints_cne import API_CONFIGS
import indicadores_cne as ind

STAGING_DIR = "/opt/airflow/dags/datos_cne"
REPORTE_DIR = "/opt/airflow/dags/reportes"
TABLA_CMF = "cmf_indicadores_gestion"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("La variable de entorno DATABASE_URL no está configurada.")
    return sqlalchemy.create_engine(db_url)


def _leer(tipo: str, resumen: dict) -> pd.DataFrame:
    """Lee solo tablas cargadas en esta ejecución, para no calcular sobre datos antiguos."""
    if tipo not in resumen:
        print(f"Aviso: '{tipo}' no se cargó en esta ejecución.")
        return pd.DataFrame()
    with _engine().connect() as conn:
        return pd.read_sql(sqlalchemy.text(f"SELECT * FROM cne_ind_{tipo}"), conn)


def _a_tabla(payload) -> pd.DataFrame:
    if isinstance(payload, list):
        return pd.json_normalize(payload) if payload else pd.DataFrame()
    if isinstance(payload, dict):
        listas = [k for k, v in payload.items() if isinstance(v, list)]
        if listas:
            return pd.json_normalize(payload[listas[0]])
        if payload and all(isinstance(v, dict) for v in payload.values()):
            return pd.json_normalize(list(payload.values()))
        return pd.json_normalize([payload])
    return pd.DataFrame()


def _guardar(df: pd.DataFrame, tabla: str, archivo: str):
    df = df.assign(fecha_calculo=datetime.now())
    df.to_sql(tabla, con=_engine(), if_exists="replace", index=False)
    os.makedirs(REPORTE_DIR, exist_ok=True)
    df.to_csv(os.path.join(REPORTE_DIR, archivo), index=False)
    print(f"Guardado: {len(df)} filas en tabla '{tabla}' y en {REPORTE_DIR}/{archivo}")


@dag(
    dag_id="etl_cne_indicadores",
    default_args=DEFAULT_ARGS,
    description="ETL CNE (combustibles, generación, mercado) + indicadores y relación con CMF",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etls", "chile", "apis", "indicadores", "cne"],
)
def etl_cne_indicadores():

    # ---------------------------------------------------------------- E T L
    @task()
    def verificar_login():
        """Detiene el DAG antes de empezar si el login falla. El token no se guarda en XCom."""
        try:
            ClienteCNE.desde_entorno().login()
        except CNEAuthError as e:  # sin reintentos: repetir un login rechazado podría bloquear la cuenta
            raise AirflowFailException(str(e)) from e
        print("Login CNE correcto.")

    @task()
    def limpiar_staging():
        """Borra los JSON del run anterior."""
        shutil.rmtree(STAGING_DIR, ignore_errors=True)
        os.makedirs(STAGING_DIR, exist_ok=True)

    @task(max_active_tis_per_dag=4)
    def extract_data(config: dict) -> dict:
        """Descarga un endpoint con un token nuevo y guarda el JSON crudo en un archivo."""
        meta = {"id_fuente": config["id_fuente"], "tipo": config["tipo"]}
        cliente = ClienteCNE.desde_entorno()
        respuestas = []
        for url in config["urls"]:
            for intento in range(1, 4):
                try:
                    respuestas.append(cliente.obtener(url))
                    break
                except CNEAuthError as e:
                    raise AirflowFailException(str(e)) from e
                except CNERespuestaError as e:
                    print(f"Aviso: [{meta['id_fuente']}] {e}. Se omite.")
                    break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    print(f"Aviso: [{meta['id_fuente']}] intento {intento}/3 falló: {type(e).__name__}")
                    if intento < 3:
                        time.sleep(30 * intento)
                except requests.exceptions.RequestException as e:
                    print(f"Aviso: [{meta['id_fuente']}] error {type(e).__name__}: {e}. Se omite.")
                    break
        if not respuestas:
            return {**meta, "archivo": None}
        # Va a un archivo porque estaciones es demasiado grande para XCom
        archivo = os.path.join(STAGING_DIR, f"{meta['id_fuente']}.raw.json")
        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(respuestas, f, ensure_ascii=False)
        return {**meta, "archivo": archivo}

    @task()
    def transform_data(extract_result: dict) -> dict:
        """Convierte el JSON crudo en tabla y lo guarda en un archivo por fuente."""
        meta = {k: extract_result[k] for k in ("id_fuente", "tipo")}
        if not extract_result.get("archivo"):
            return {**meta, "status": "skipped"}
        with open(extract_result["archivo"], encoding="utf-8") as f:
            respuestas = json.load(f)

        conversor = ind.aplanar_estaciones if meta["tipo"] == "estaciones" else _a_tabla
        partes = [p for p in map(conversor, respuestas) if not p.empty]
        if not partes:
            print(f"Aviso: [{meta['id_fuente']}] sin filas.")
            return {**meta, "status": "empty"}
        df = pd.concat(partes, ignore_index=True)
        # Listas o dicts anidados no caben en una columna de PostgreSQL
        for c in df.columns:
            if df[c].map(lambda v: isinstance(v, (list, dict))).any():
                df[c] = df[c].map(lambda v: json.dumps(v, ensure_ascii=False)
                                  if isinstance(v, (list, dict)) else v)
        df = df.drop_duplicates()
        path = os.path.join(STAGING_DIR, f"{meta['tipo']}.tabla.json")
        df.to_json(path, orient="records", force_ascii=False)
        print(f"OK [{meta['id_fuente']}]: {len(df)} filas. Columnas: {list(df.columns)}")
        return {**meta, "status": "success", "filas": len(df), "archivo": path}

    @task()
    def load_to_postgres(transform_results: list[dict]) -> dict:
        """Carga cada fuente en cne_ind_<tipo> y documenta la estructura recibida."""
        engine = _engine()
        resumen, esquema = {}, {}
        for r in transform_results:
            if r.get("status") != "success":
                print(f"Aviso: [{r['id_fuente']}] no se carga (status={r.get('status')}).")
                continue
            df = pd.read_json(r["archivo"], orient="records", dtype=False)
            esquema[r["tipo"]] = {"filas": len(df), "columnas": list(df.columns),
                                  "ejemplo": json.loads(df.head(2).to_json(orient="records"))}
            df = df.astype(str)  # igual que en CMF: se tipa al calcular los indicadores
            df.to_sql(f"cne_ind_{r['tipo']}", con=engine, if_exists="replace", index=False,
                      method="multi", chunksize=1000)
            resumen[r["tipo"]] = len(df)
            print(f"Carga exitosa: {len(df)} registros en cne_ind_{r['tipo']}")
        if not resumen:
            raise ValueError("Ninguna fuente CNE devolvió datos.")
        os.makedirs(REPORTE_DIR, exist_ok=True)
        with open(os.path.join(REPORTE_DIR, "cne_esquema.json"), "w", encoding="utf-8") as f:
            json.dump(esquema, f, ensure_ascii=False, indent=2)
        return resumen

    # ----------------------------------------------------- I N D I C A D O R E S
    @task()
    def kpi_estaciones(resumen: dict) -> dict:
        """Indicador 1: precios por combustible y región, y participación de distribuidores."""
        estaciones = _leer("estaciones", resumen)
        precios = ind.kpi_precios(estaciones)
        distribuidores = ind.kpi_distribuidores(estaciones)
        if not precios.empty:
            _guardar(precios, "cne_precios_combustibles", "cne_precios_combustibles.csv")
            print(precios[precios["region"] == "NACIONAL"].to_string())
        if not distribuidores.empty:
            _guardar(distribuidores, "cne_participacion_distribuidores", "cne_participacion_distribuidores.csv")
        return {"precios": len(precios), "distribuidores": len(distribuidores)}

    @task()
    def kpi_generacion_anual(resumen: dict) -> list[dict]:
        """Indicador 2: generación de Sistemas Medianos, % renovable y capacidad acumulada."""
        df = ind.consolidar_anual(ind.kpi_generacion(_leer("generacion", resumen)),
                                  ind.kpi_capacidad(_leer("capacidad", resumen)))
        return json.loads(df.drop(columns="variacion_anual").to_json(orient="records"))

    @task()
    def kpi_mercado_anual(resumen: dict) -> list[dict]:
        """Indicador 3: ventas, comercio exterior y relación con el PIB regional EGA."""
        df = ind.kpi_mercado(_leer("ventas", resumen), _leer("importacion", resumen),
                             _leer("exportacion", resumen), _leer("pib_regional", resumen))
        return json.loads(df.to_json(orient="records"))

    @task()
    def consolidar_indicadores(generacion: list[dict], mercado: list[dict], estaciones: dict) -> list[dict]:
        """Une las series anuales CNE; avisa cuáles no se pudieron calcular."""
        df = ind.consolidar_anual(pd.DataFrame(generacion), pd.DataFrame(mercado))
        print(f"Estaciones: {estaciones}")
        faltantes = sorted(set(ind.ESPERADOS) - set(df["indicador"]) if not df.empty else ind.ESPERADOS)
        if faltantes:
            print(f"Aviso: indicadores no calculados: {faltantes}. Revisa {REPORTE_DIR}/cne_esquema.json")
        if df.empty:
            raise ValueError("No se calculó ninguna serie anual CNE: revisa la estructura en cne_esquema.json.")
        _guardar(df, "cne_indicadores_anuales", "cne_indicadores_anuales.csv")
        print(df.pivot_table(index="anio", columns="indicador", values="valor").round(2).to_string())
        return json.loads(df.to_json(orient="records"))

    # ------------------------------------------------------ R E L A C I Ó N
    @task()
    def cruzar_con_cmf(anuales: list[dict]):
        """Relaciona cada indicador CNE con ROA, ROE y apalancamiento de cada banco."""
        engine = _engine()
        if not sqlalchemy.inspect(engine).has_table(TABLA_CMF):
            raise ValueError(f"No existe la tabla {TABLA_CMF}: ejecuta primero el DAG etl_cmf_indicadores.")
        with engine.connect() as conn:
            cmf = pd.read_sql(sqlalchemy.text(f"SELECT * FROM {TABLA_CMF}"), conn)

        panel = ind.panel_cne_cmf(cmf, pd.DataFrame(anuales))
        if panel.empty:
            raise ValueError("CNE y CMF no tienen años en común.")
        _guardar(panel, "cne_cmf_panel", "cne_cmf_panel.csv")

        corr = ind.correlaciones(panel)
        if corr.empty:
            print("Aviso: no hay suficientes años en común para calcular correlaciones.")
            return
        _guardar(corr, "cne_cmf_correlaciones", "cne_cmf_correlaciones.csv")
        print("Pares del promedio de bancos (SISTEMA), ordenados por q_valor:\n",
              corr[corr["institucion"] == "SISTEMA"].head(15).to_string())

    # ------------------------------------------------------------- F L U J O
    login = verificar_login()
    staging = limpiar_staging()
    login >> staging
    extraidos = extract_data.expand(config=API_CONFIGS)
    staging >> extraidos
    transformados = transform_data.expand(extract_result=extraidos)
    carga = load_to_postgres(transformados)

    anuales = consolidar_indicadores(kpi_generacion_anual(carga), kpi_mercado_anual(carga),
                                     kpi_estaciones(carga))
    cruzar_con_cmf(anuales)


etl_cne_indicadores()
