# Archivo: dags/etl_cmf_indicadores.py
"""
ETL CMF Chile + 3 indicadores de gestión.

Flujo visible en Airflow:
  aplicar_ddl -> limpiar_staging -> extract_data[] -> transform_data[] -> load_to_postgres
      -> preparar_base -> (kpi_roa | kpi_roe | kpi_apalancamiento) -> consolidar_indicadores
      -> resumen_ejecucion

Si un endpoint falla, esa fuente reintenta y queda en rojo; las demás continúan y la fuente conserva su
último dato válido. resumen_ejecucion marca el run como fallido para que el error no pase inadvertido.
Cada tarea registra su ejecución en audit.etl_audit.

Parámetro al ejecutar a mano:
  fuente_falla  id de una fuente para la demostración de falla controlada, por ejemplo balances_001_2020
"""
from datetime import datetime, timedelta
import glob
import json
import os
import shutil

import pandas as pd
import requests
import sqlalchemy

try:  # Airflow 3
    from airflow.sdk import Param, dag, get_current_context, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException

from endpoints_indicadores import API_CONFIGS
from etl_comun import al_fallar, auditoria, ejecutar_sql
import indicadores as ind
from transform_cne import _pct_nulos, contar_filas

STAGING_DIR = "/opt/airflow/dags/datos_indicadores"  # propio: no choca con etl_CMF
REPORTE_DIR = "/opt/airflow/dags/reportes"
TABLA_INDICADORES = "cmf_indicadores_gestion"
DAG_ID = "etl_cmf_indicadores"
URL_DEMO_FALLA = "https://api.cmfchile.cl/api-sbifv3/recursos_api/ruta-inexistente-demo-falla"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": al_fallar,
}


def _engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("La variable de entorno DATABASE_URL no está configurada.")
    return sqlalchemy.create_engine(db_url)


def _reemplazar_fuentes(engine, tabla: str, df: pd.DataFrame) -> int:
    """Reemplaza solo las filas de las fuentes recibidas. Devuelve cuántas filas de otras fuentes se conservan."""
    with engine.begin() as conn:
        inspector = sqlalchemy.inspect(conn)
        if not inspector.has_table(tabla):
            df.to_sql(tabla, con=conn, index=False, method="multi", chunksize=1000)
            return 0
        columnas = {c["name"] for c in inspector.get_columns(tabla)}
        for columna in df.columns:
            if columna not in columnas:  # la CMF puede agregar campos
                conn.execute(sqlalchemy.text(f'ALTER TABLE "{tabla}" ADD COLUMN "{columna}" TEXT'))
        conn.execute(sqlalchemy.text(f'DELETE FROM "{tabla}" WHERE id_fuente = ANY(:fuentes)'),
                     {"fuentes": df["id_fuente"].unique().tolist()})
        conservadas = conn.execute(sqlalchemy.text(f'SELECT COUNT(*) FROM "{tabla}"')).scalar()
        df.to_sql(tabla, con=conn, if_exists="append", index=False, method="multi", chunksize=1000)
    return conservadas


@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="ETL CMF (balances, resultados, adecuación) + indicadores ROA, ROE y apalancamiento",
    # Mensual: la CMF publica cada mes, con cerca de un mes de desfase. Día 10, 06:00 de Chile.
    schedule="0 9 10 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={"fuente_falla": Param("", type="string", description="Fuente con URL incorrecta, para la demostración")},
    tags=["etls", "chile", "apis", "indicadores"],
)
def etl_cmf_indicadores():

    # ---------------------------------------------------------------- E T L
    @task()
    def aplicar_ddl():
        """Crea la tabla de auditoría si no existe. Se puede repetir sin efectos adicionales."""
        ejecutar_sql("ddl.sql")

    @task()
    def limpiar_staging():
        """Borra los JSON del run anterior para que los datos no se dupliquen."""
        with auditoria(get_current_context(), capa="preparacion"):
            shutil.rmtree(STAGING_DIR, ignore_errors=True)
            os.makedirs(STAGING_DIR, exist_ok=True)
        return STAGING_DIR

    @task(max_active_tis_per_dag=4)  # limita la concurrencia contra la API
    def extract_data(config: dict) -> dict:
        """
        Consulta un endpoint de la CMF. Si hay un error, la tarea falla y Airflow la reintenta;
        las demás fuentes continúan. Solo las fuentes opcionales se omiten sin fallar.
        """
        contexto = get_current_context()
        id_fuente = config["id_fuente"]
        base = {k: config[k] for k in ("id_fuente", "tipo", "institucion", "anio", "mes")}
        api_key = os.getenv("API_KEY") or ""
        url = config["url_template"]
        if contexto["params"].get("fuente_falla") == id_fuente:
            print(f"DEMOSTRACIÓN: se usa una URL incorrecta para '{id_fuente}'.")
            url = URL_DEMO_FALLA

        with auditoria(contexto, fuente=id_fuente, capa="extraccion") as m:
            error = None
            try:
                r = requests.get(url, params={"apikey": api_key, "formato": "json"},
                                 headers={"Accept": "application/json"}, timeout=(10, 180))
                if r.status_code >= 400:
                    error = f"HTTP {r.status_code}"
                else:
                    datos = r.json()
            except requests.exceptions.RequestException as e:  # incluye timeouts, SSL y JSON inválido
                error = f"{type(e).__name__}: {e}".replace(api_key, "***") if api_key else f"{type(e).__name__}: {e}"

            if error is None:
                m.update(leidas=contar_filas(datos), cargadas=contar_filas(datos))
                return {**base, "raw_data": datos}
            if config.get("opcional"):
                print(f"Aviso: [{id_fuente}] {error}. Es opcional: se omite.")
                m.update(estado="omitida", leidas=0, cargadas=0, mensaje=error)
                return {**base, "raw_data": None}
            # Sin encadenar el error original: su texto incluye la URL con la clave de la API
            raise RuntimeError(f"[{id_fuente}] {error}") from None

    @task(trigger_rule="all_done")  # corre para las fuentes que sí entregaron datos
    def transform_data(extract_result: dict) -> dict:
        """Normaliza el JSON a tabla, limpia y lo guarda en un archivo por fuente."""
        raw = extract_result.pop("raw_data", None)
        meta = extract_result
        if not raw:
            return {**meta, "status": "skipped"}

        with auditoria(get_current_context(), fuente=meta["id_fuente"], capa="transformacion") as m:
            if isinstance(raw, dict):
                listas = [k for k, v in raw.items() if isinstance(v, list)]
                df = pd.json_normalize(raw[listas[0]]) if listas else pd.json_normalize([raw])
            elif isinstance(raw, list):
                df = pd.json_normalize(raw)
            else:
                df = pd.DataFrame()

            if df.empty:
                m.update(estado="omitida", leidas=0, cargadas=0, mensaje="La API no devolvió filas")
                return {**meta, "status": "empty"}

            leidas, nulos = len(df), _pct_nulos(df, list(df.columns))
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
            m.update(leidas=leidas, cargadas=len(df), rechazadas=leidas - len(df), pct_nulos=nulos)
            return {**meta, "status": "success", "filas": len(df)}

    @task(trigger_rule="all_done", retries=0)
    def load_to_postgres(transform_results: list[dict]) -> dict:
        """Actualiza solo las fuentes que entregaron datos; las que fallaron conservan su último dato válido."""
        engine = _engine()
        resumen = {}
        tipos = {r["tipo"] for r in transform_results if r and r.get("status") == "success"}
        for tipo in sorted(tipos):
            with auditoria(get_current_context(), fuente=tipo, capa="carga") as m:
                archivos = glob.glob(os.path.join(STAGING_DIR, f"{tipo}__*.json"))
                df = pd.concat([pd.read_json(a, orient="records", dtype=False) for a in archivos],
                               ignore_index=True)
                df = df.astype(str)  # la API entrega todo como texto; se tipa en los indicadores
                conservadas = _reemplazar_fuentes(engine, f"cmf_ind_{tipo}", df)
                resumen[tipo] = len(df)
                m.update(leidas=len(df), cargadas=len(df), mensaje=f"{conservadas} filas conservadas de otras fuentes")
                print(f"Carga exitosa: {len(df)} registros en cmf_ind_{tipo}; se conservan {conservadas} de otras fuentes")
        if not resumen:
            raise ValueError("Ninguna fuente devolvió datos: las tablas conservan su último dato válido.")
        return resumen

    # ----------------------------------------------------- I N D I C A D O R E S
    @task()
    def preparar_base(resumen_carga: dict) -> list[dict]:
        """Lee balances y resultados desde PostgreSQL y arma la tabla banco-año."""
        engine = _engine()
        with auditoria(get_current_context(), capa="indicadores") as m:
            with engine.connect() as conn:
                balances = pd.read_sql(sqlalchemy.text("SELECT * FROM cmf_ind_balances"), conn)
                resultados = pd.read_sql(sqlalchemy.text("SELECT * FROM cmf_ind_resultados"), conn)
            base = ind.base_indicadores(balances, resultados)
            cuentas = base[["activos", "pasivos", "patrimonio", "utilidad"]]
            faltantes = base[cuentas.isna().any(axis=1)]
            if not faltantes.empty:
                print("Aviso: bancos/años con cuentas faltantes:\n", faltantes.to_string())
            print(base.to_string())
            m.update(leidas=len(balances) + len(resultados), cargadas=len(base), rechazadas=len(faltantes),
                     pct_nulos=round(float(cuentas.isna().to_numpy().mean()) * 100, 2))
            return json.loads(base.to_json(orient="records"))

    @task()
    def kpi_roa(base: list[dict]) -> list[dict]:
        """Indicador 1: ROA = Utilidad / Activos totales (%)."""
        with auditoria(get_current_context(), capa="indicadores") as m:
            salida = json.loads(ind.kpi_roa(pd.DataFrame(base)).to_json(orient="records"))
            m.update(leidas=len(base), cargadas=len(salida))
            return salida

    @task()
    def kpi_roe(base: list[dict]) -> list[dict]:
        """Indicador 2: ROE = Utilidad / Patrimonio (%)."""
        with auditoria(get_current_context(), capa="indicadores") as m:
            salida = json.loads(ind.kpi_roe(pd.DataFrame(base)).to_json(orient="records"))
            m.update(leidas=len(base), cargadas=len(salida))
            return salida

    @task()
    def kpi_apalancamiento(base: list[dict]) -> list[dict]:
        """Indicador 3: Apalancamiento = Pasivos / Patrimonio (veces)."""
        with auditoria(get_current_context(), capa="indicadores") as m:
            salida = json.loads(ind.kpi_apalancamiento(pd.DataFrame(base)).to_json(orient="records"))
            m.update(leidas=len(base), cargadas=len(salida))
            return salida

    @task()
    def consolidar_indicadores(roa: list[dict], roe: list[dict], apal: list[dict]):
        """Une los 3 indicadores, agrega variación anual y semáforo, y los guarda."""
        with auditoria(get_current_context(), capa="indicadores") as m:
            df = ind.consolidar(pd.DataFrame(roa), pd.DataFrame(roe), pd.DataFrame(apal))
            df["fecha_calculo"] = datetime.now()
            df.to_sql(TABLA_INDICADORES, con=_engine(), if_exists="replace", index=False)

            os.makedirs(REPORTE_DIR, exist_ok=True)
            df.to_csv(os.path.join(REPORTE_DIR, "indicadores_gestion.csv"), index=False)
            pivote = df.pivot_table(index=["institucion", "anio"], columns="indicador", values="valor")
            print("Indicadores de gestión:\n", pivote.round(2).to_string())
            print(f"Guardado en tabla '{TABLA_INDICADORES}' y en {REPORTE_DIR}/indicadores_gestion.csv")
            m.update(leidas=len(roa) + len(roe) + len(apal), cargadas=len(df),
                     pct_nulos=round(float(df["valor"].isna().mean()) * 100, 2))

    @task(trigger_rule="all_done", retries=0)
    def resumen_ejecucion():
        """Marca el run como fallido si alguna tarea falló, después de dejar correr todo lo demás."""
        run_id = get_current_context()["ti"].run_id
        with _engine().connect() as conn:
            filas = pd.read_sql(sqlalchemy.text("""
                SELECT task_id, fuente, estado, COUNT(*) AS n, MAX(LEFT(mensaje, 100)) AS mensaje
                FROM audit.etl_audit WHERE dag_id = :d AND run_id = :r GROUP BY 1, 2, 3 ORDER BY 1, 2
            """), conn, params={"d": DAG_ID, "r": run_id})
        print(filas.to_string())
        fallidas = filas[filas["estado"] == "failed"]
        if not fallidas.empty:
            exitos = set(filas.loc[filas["estado"] == "success", "fuente"].dropna())
            fuentes = sorted(set(filas.loc[filas["estado"] == "error_intento", "fuente"].dropna()) - exitos)
            raise AirflowFailException(f"Fuentes con falla: {fuentes or 'revisar tareas'}. "
                                       f"Tareas: {sorted(set(fallidas['task_id']))}. El resto del flujo continuó.")

    # ------------------------------------------------------------- F L U J O
    ddl = aplicar_ddl()
    staging = limpiar_staging()
    ddl >> staging
    extraidos = extract_data.expand(config=API_CONFIGS)
    staging >> extraidos
    transformados = transform_data.expand(extract_result=extraidos)
    carga = load_to_postgres(transformados)

    base = preparar_base(carga)
    fin = consolidar_indicadores(kpi_roa(base), kpi_roe(base), kpi_apalancamiento(base))
    fin >> resumen_ejecucion()


etl_cmf_indicadores()
