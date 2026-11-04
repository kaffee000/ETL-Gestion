# Archivo: dags/etl_cne.py
"""
ETL de la API CNE hacia las capas raw, staging y dw de PostgreSQL.

  aplicar_ddl -> validar_api -> extraer_<fuente> -> limpiar_<fuente> -> cargar_dw -> resumen_ejecucion

Cada fuente es una rama independiente: si una falla, reintenta y las demás continúan.
Al terminar cargar_dw se dispara automáticamente el DAG indicadores_gestion.

Parámetros al ejecutar a mano:
  anio_desde, anio_hasta  rango de años que se recarga en dw (recarga histórica)
  fuente_falla            nombre de una fuente para la demostración de falla controlada
"""
from datetime import datetime, timedelta
import json
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from sqlalchemy import text

try:  # Airflow 3
    from airflow.sdk import Asset, Param, dag, get_current_context, task
except ImportError:  # Airflow 2.x
    from airflow.datasets import Dataset as Asset
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException

from cne_cliente import ClienteCNE, CNEAuthError
from endpoints_cne import API_BASE, API_CONFIGS
from etl_comun import al_fallar, auditoria, ejecutar_sql, engine
import transform_cne as tr

ACTIVO_DW_CNE = Asset("dw_cne")
URL_DEMO_FALLA = f"{API_BASE}/ruta-inexistente-demo-falla"
RAW_EJECUCIONES_A_CONSERVAR = 7


def _hoy():
    return datetime.now(ZoneInfo("America/Santiago")).date()


# Fuente -> (función de limpieza, tablas destino en staging, filtro que se reemplaza)
LIMPIEZAS = {
    "regiones": (tr.limpiar_regiones, "cne_region", None),
    "estaciones": (lambda r: tr.limpiar_estaciones(r, _hoy()), "cne_estacion_precio", None),
    "generacion": (tr.limpiar_generacion, "cne_generacion", None),
    "capacidad": (tr.limpiar_capacidad, "cne_capacidad", None),
    "ventas": (tr.limpiar_ventas, "cne_venta", None),
    "importacion": (lambda r: tr.limpiar_comercio(r, "I"), "cne_comercio", "flujo = 'I'"),
    "exportacion": (lambda r: tr.limpiar_comercio(r, "E"), "cne_comercio", "flujo = 'E'"),
    "pib_regional": (tr.limpiar_pib, "cne_pib_regional", None),
}

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


@dag(
    dag_id="etl_cne",
    default_args=DEFAULT_ARGS,
    description="ETL API CNE: raw -> staging -> dw, con auditoría y fuentes independientes",
    # Diario porque los precios cambian a diario y la API no guarda historia; las demás fuentes
    # se actualizan con menos frecuencia, pero recargarlas no duplica datos.
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,  # limita las consultas simultáneas a la API
    params={
        "anio_desde": Param(1900, type="integer", description="Primer año que se recarga en dw"),
        "anio_hasta": Param(2100, type="integer", description="Último año que se recarga en dw"),
        "fuente_falla": Param("", type="string", description="Fuente con URL incorrecta, para la demostración"),
    },
    tags=["etls", "chile", "apis", "cne"],
)
def etl_cne():

    @task()
    def aplicar_ddl():
        """Crea o actualiza esquemas y tablas. Se puede repetir sin efectos adicionales."""
        ejecutar_sql("ddl.sql")

    @task()
    def validar_api():
        """Comprueba que la API responde y que el login funciona antes de extraer."""
        contexto = get_current_context()
        with auditoria(contexto, fuente="api_cne", capa="validacion") as m:
            r = requests.get(f"{API_BASE}/region", headers={"Accept": "application/json"}, timeout=(10, 60))
            r.raise_for_status()
            if not isinstance(r.json(), list) or not r.json():
                raise ValueError("La API respondió, pero sin el listado esperado de regiones.")
            try:
                ClienteCNE.desde_entorno().login()
            except CNEAuthError as e:  # sin reintentos: repetir un login rechazado podría bloquear la cuenta
                raise AirflowFailException(str(e)) from e
            m["leidas"] = len(r.json())
            print(f"API CNE disponible y login correcto. Regiones: {m['leidas']}")

    @task()
    def extraer(config: dict):
        """Guarda cada respuesta tal como llega en raw.cne_respuesta, con fecha y hora."""
        contexto = get_current_context()
        run_id = contexto["ti"].run_id
        id_fuente = config["id_fuente"]
        urls = config["urls"]
        if contexto["params"].get("fuente_falla") == id_fuente:
            print(f"DEMOSTRACIÓN: se usa una URL incorrecta para '{id_fuente}'.")
            urls = [URL_DEMO_FALLA]

        with auditoria(contexto, fuente=id_fuente, capa="raw") as m:
            cliente = ClienteCNE.desde_entorno()
            total = 0
            for url in urls:
                try:
                    payload = cliente.obtener(url)
                except CNEAuthError as e:
                    raise AirflowFailException(str(e)) from e
                filas = tr.contar_filas(payload)
                total += filas
                with engine().begin() as conn:
                    conn.execute(text("""
                        INSERT INTO raw.cne_respuesta (run_id, id_fuente, url, payload, filas, fecha_extraccion)
                        VALUES (:run_id, :id_fuente, :url, CAST(:payload AS JSONB), :filas, now())
                        ON CONFLICT (run_id, id_fuente, url) DO UPDATE SET
                            payload = EXCLUDED.payload, filas = EXCLUDED.filas,
                            fecha_extraccion = EXCLUDED.fecha_extraccion
                    """), {"run_id": run_id, "id_fuente": id_fuente, "url": url, "filas": filas,
                           "payload": json.dumps(payload, ensure_ascii=False).replace("\\u0000", "")})
            m.update(leidas=total, cargadas=total)
            print(f"OK [{id_fuente}]: {total} filas en raw.cne_respuesta")

    @task()
    def limpiar(fuente: str):
        """Lee la capa raw de esta ejecución, limpia, valida y reemplaza los datos en staging."""
        contexto = get_current_context()
        funcion, tabla, filtro = LIMPIEZAS[fuente]
        with auditoria(contexto, fuente=fuente, capa="staging") as m:
            with engine().connect() as conn:
                filas = conn.execute(text("""
                    SELECT payload FROM raw.cne_respuesta WHERE run_id = :run_id AND id_fuente = :fuente
                """), {"run_id": contexto["ti"].run_id, "fuente": fuente}).fetchall()
            res = funcion([f[0] for f in filas])
            if res.tabla.empty:
                raise ValueError(f"{fuente}: no quedaron filas después de limpiar.")
            with engine().begin() as conn:
                condicion = f"WHERE {filtro}" if filtro else ""
                conn.execute(text(f"DELETE FROM staging.{tabla} {condicion}"))
                res.tabla.to_sql(tabla, con=conn, schema="staging", if_exists="append", index=False,
                                 method="multi", chunksize=1000)
            m.update(leidas=res.leidas, cargadas=len(res.tabla), rechazadas=res.rechazadas,
                     pct_nulos=res.pct_nulos, mensaje=res.detalle or None)
            print(f"OK [{fuente}]: leídas {res.leidas}, cargadas {len(res.tabla)}, rechazadas {res.rechazadas}, "
                  f"nulos {res.pct_nulos}%. {res.detalle}")

    @task(trigger_rule="all_done", outlets=[ACTIVO_DW_CNE])
    def cargar_dw():
        """Pasa staging a dw aunque alguna fuente haya fallado; esa fuente conserva su último dato válido."""
        contexto = get_current_context()
        p = contexto["params"]
        with auditoria(contexto, capa="dw") as m:
            ejecutar_sql("dw_cne.sql", anio_desde=p["anio_desde"], anio_hasta=p["anio_hasta"])
            tablas = ["fact_precio_combustible", "fact_generacion", "fact_capacidad", "fact_venta_combustible",
                      "fact_comercio_exterior", "fact_pib_regional"]
            with engine().connect() as conn:
                conteo = {t: conn.execute(text(f"SELECT COUNT(*) FROM dw.{t}")).scalar() for t in tablas}
            m["cargadas"] = sum(conteo.values())
            print(f"Hechos en dw: {conteo}")

    @task(trigger_rule="all_done", retries=0)
    def resumen_ejecucion():
        """Conserva las últimas ejecuciones en raw y marca el run como fallido si alguna fuente falló."""
        run_id = get_current_context()["ti"].run_id
        with engine().begin() as conn:
            conn.execute(text("""
                DELETE FROM raw.cne_respuesta WHERE run_id NOT IN (
                    SELECT run_id FROM raw.cne_respuesta GROUP BY run_id
                    ORDER BY MAX(fecha_extraccion) DESC LIMIT :n)
            """), {"n": RAW_EJECUCIONES_A_CONSERVAR})
            resumen = pd.read_sql(text("""
                SELECT task_id, estado, intento, filas_leidas, filas_cargadas, filas_rechazadas, pct_nulos, duracion_s
                FROM audit.etl_audit WHERE dag_id = 'etl_cne' AND run_id = :r ORDER BY inicio
            """), conn, params={"r": run_id})
        print(resumen.to_string())
        fallidas = resumen.loc[resumen["estado"] == "failed", "task_id"].unique().tolist()
        if fallidas:
            raise AirflowFailException(f"Tareas fallidas en esta ejecución: {fallidas}. El resto del flujo continuó.")

    # ------------------------------------------------------------- F L U J O
    ddl = aplicar_ddl()
    validacion = validar_api()
    carga = cargar_dw()
    fin = resumen_ejecucion()
    ddl >> validacion

    for config in API_CONFIGS:
        fuente = config["id_fuente"]
        extraccion = extraer.override(task_id=f"extraer_{fuente}")(config)
        validacion >> extraccion
        if fuente in LIMPIEZAS:
            extraccion >> limpiar.override(task_id=f"limpiar_{fuente}")(fuente) >> carga
        else:  # dimensiones de referencia: quedan disponibles en raw
            extraccion >> carga
    carga >> fin


etl_cne()
