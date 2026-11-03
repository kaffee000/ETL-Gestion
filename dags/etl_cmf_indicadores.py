# Archivo: dags/etl_cmf_indicadores.py
"""
ETL CMF Chile + 3 indicadores de gestión.

Flujo visible en Airflow:
  limpiar_staging -> extract_data[] -> transform_data[] -> load_to_postgres
      -> preparar_base -> (kpi_roa | kpi_roe | kpi_apalancamiento) -> consolidar_indicadores
"""
from datetime import datetime, timedelta
import glob
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

from endpoints_indicadores import API_CONFIGS
import indicadores as ind

STAGING_DIR = "/opt/airflow/dags/datos_indicadores"  # propio: no choca con etl_CMF
REPORTE_DIR = "/opt/airflow/dags/reportes"
TABLA_INDICADORES = "cmf_indicadores_gestion"

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


@dag(
    dag_id="etl_cmf_indicadores",
    default_args=DEFAULT_ARGS,
    description="ETL CMF (balances, resultados, adecuación) + indicadores ROA, ROE y apalancamiento",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etls", "chile", "apis", "indicadores"],
)
def etl_cmf_indicadores():

    # ---------------------------------------------------------------- E T L
    @task()
    def limpiar_staging():
        """Borra los JSON del run anterior para que los datos no se dupliquen."""
        shutil.rmtree(STAGING_DIR, ignore_errors=True)
        os.makedirs(STAGING_DIR, exist_ok=True)
        return STAGING_DIR

    @task(max_active_tis_per_dag=4)  # limita la concurrencia contra la API
    def extract_data(config: dict) -> dict:
        """Consulta un endpoint de la CMF; errores de red o HTTP se registran y se omiten."""
        base = {k: config[k] for k in ("id_fuente", "tipo", "institucion", "anio", "mes")}
        api_key = os.getenv("API_KEY") or ""
        intentos = 3
        for intento in range(1, intentos + 1):  # la API CMF es lenta: reintenta timeouts/cortes
            try:
                r = requests.get(config["url_template"], params={"apikey": api_key, "formato": "json"},
                                 headers={"Accept": "application/json"}, timeout=(10, 180))
                if r.status_code in (404, 500, 502, 503, 504):
                    print(f"Aviso: [{config['id_fuente']}] HTTP {r.status_code}. Se omite.")
                    return {**base, "raw_data": None}
                r.raise_for_status()
                return {**base, "raw_data": r.json()}
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                msg = str(e).replace(api_key, "***") if api_key else str(e)  # no exponer la key
                print(f"Aviso: [{config['id_fuente']}] intento {intento}/{intentos} falló: {msg}")
                if intento < intentos:
                    time.sleep(30 * intento)
            except requests.exceptions.RequestException as e:
                msg = str(e).replace(api_key, "***") if api_key else str(e)
                print(f"Aviso: error en [{config['id_fuente']}]: {msg}. Se omite.")
                return {**base, "raw_data": None}
        print(f"Aviso: [{config['id_fuente']}] sin respuesta tras {intentos} intentos. Se omite.")
        return {**base, "raw_data": None}

    @task()
    def transform_data(extract_result: dict) -> dict:
        """Normaliza el JSON a tabla, limpia y lo guarda en un archivo por fuente."""
        raw = extract_result.pop("raw_data", None)
        meta = extract_result
        if not raw:
            return {**meta, "status": "skipped"}

        if isinstance(raw, dict):
            listas = [k for k, v in raw.items() if isinstance(v, list)]
            df = pd.json_normalize(raw[listas[0]]) if listas else pd.json_normalize([raw])
        elif isinstance(raw, list):
            df = pd.json_normalize(raw)
        else:
            df = pd.DataFrame()

        if df.empty:
            return {**meta, "status": "empty"}

        df = df.drop_duplicates().fillna("")
        # Llaves de negocio para poder agrupar en los indicadores
        df["institucion"] = meta["institucion"]
        df["anio"] = meta["anio"]
        if meta.get("mes") and "mes" not in [c.lower() for c in df.columns]:
            df["mes"] = meta["mes"]
        df["id_fuente"] = meta["id_fuente"]

        # Un archivo por fuente: no hay escrituras concurrentes sobre el mismo archivo
        path = os.path.join(STAGING_DIR, f"{meta['tipo']}__{meta['id_fuente']}.json")
        df.to_json(path, orient="records", force_ascii=False)
        print(f"OK [{meta['id_fuente']}]: {len(df)} filas -> {path}")
        return {**meta, "status": "success", "filas": len(df)}

    @task()
    def load_to_postgres(transform_results: list[dict]) -> dict:
        """Consolida los archivos de cada categoría y los carga en cmf_ind_<tipo>."""
        engine = _engine()
        resumen = {}
        tipos = {r["tipo"] for r in transform_results if r.get("status") == "success"}
        for tipo in sorted(tipos):
            archivos = glob.glob(os.path.join(STAGING_DIR, f"{tipo}__*.json"))
            df = pd.concat([pd.read_json(a, orient="records", dtype=False) for a in archivos],
                           ignore_index=True)
            df = df.astype(str)  # la API entrega todo como texto; se tipa en los indicadores
            df.to_sql(f"cmf_ind_{tipo}", con=engine, if_exists="replace", index=False,
                      method="multi", chunksize=1000)
            resumen[tipo] = len(df)
            print(f"Carga exitosa: {len(df)} registros en cmf_ind_{tipo}")
        if not resumen:
            raise ValueError("Ninguna fuente devolvió datos: revisa API_KEY y la conexión a la API.")
        return resumen

    # ----------------------------------------------------- I N D I C A D O R E S
    @task()
    def preparar_base(resumen_carga: dict) -> list[dict]:
        """Lee balances y resultados desde PostgreSQL y arma la tabla banco-año."""
        engine = _engine()
        with engine.connect() as conn:
            balances = pd.read_sql(sqlalchemy.text("SELECT * FROM cmf_ind_balances"), conn)
            resultados = pd.read_sql(sqlalchemy.text("SELECT * FROM cmf_ind_resultados"), conn)
        base = ind.base_indicadores(balances, resultados)
        faltantes = base[base[["activos", "pasivos", "patrimonio", "utilidad"]].isna().any(axis=1)]
        if not faltantes.empty:
            print("Aviso: bancos/años con cuentas faltantes:\n", faltantes.to_string())
        print(base.to_string())
        return json.loads(base.to_json(orient="records"))

    @task()
    def kpi_roa(base: list[dict]) -> list[dict]:
        """Indicador 1: ROA = Utilidad / Activos totales (%)."""
        return json.loads(ind.kpi_roa(pd.DataFrame(base)).to_json(orient="records"))

    @task()
    def kpi_roe(base: list[dict]) -> list[dict]:
        """Indicador 2: ROE = Utilidad / Patrimonio (%)."""
        return json.loads(ind.kpi_roe(pd.DataFrame(base)).to_json(orient="records"))

    @task()
    def kpi_apalancamiento(base: list[dict]) -> list[dict]:
        """Indicador 3: Apalancamiento = Pasivos / Patrimonio (veces)."""
        return json.loads(ind.kpi_apalancamiento(pd.DataFrame(base)).to_json(orient="records"))

    @task()
    def consolidar_indicadores(roa: list[dict], roe: list[dict], apal: list[dict]):
        """Une los 3 indicadores, agrega variación anual y semáforo, y los guarda."""
        df = ind.consolidar(pd.DataFrame(roa), pd.DataFrame(roe), pd.DataFrame(apal))
        df["fecha_calculo"] = datetime.now()
        df.to_sql(TABLA_INDICADORES, con=_engine(), if_exists="replace", index=False)

        os.makedirs(REPORTE_DIR, exist_ok=True)
        df.to_csv(os.path.join(REPORTE_DIR, "indicadores_gestion.csv"), index=False)
        pivote = df.pivot_table(index=["institucion", "anio"], columns="indicador", values="valor")
        print("Indicadores de gestión:\n", pivote.round(2).to_string())
        print(f"Guardado en tabla '{TABLA_INDICADORES}' y en {REPORTE_DIR}/indicadores_gestion.csv")

    # ------------------------------------------------------------- F L U J O
    staging = limpiar_staging()
    extraidos = extract_data.expand(config=API_CONFIGS)
    staging >> extraidos
    transformados = transform_data.expand(extract_result=extraidos)
    carga = load_to_postgres(transformados)

    base = preparar_base(carga)
    consolidar_indicadores(kpi_roa(base), kpi_roe(base), kpi_apalancamiento(base))


etl_cmf_indicadores()
