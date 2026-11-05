# Archivo: dags/indicadores_gestion.py
"""
DAG de indicadores: calcula los KPIs en SQL sobre dw y los deja en mart.

  aplicar_ddl -> cargar_finanzas_cmf -> calcular_kpis_sql -> relacion_cne_cmf -> exportar_reportes

Se dispara automáticamente cuando etl_cne termina de cargar dw. Si los datos de la CMF no están
disponibles, calcula los KPIs de la CNE con lo que haya.
"""
from datetime import datetime, timedelta
import os

import pandas as pd
import sqlalchemy
from sqlalchemy import text

try:  # Airflow 3
    from airflow.sdk import Asset, dag, get_current_context, task
except ImportError:  # Airflow 2.x
    from airflow.datasets import Dataset as Asset
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context

from etl_comun import REPORTE_DIR, al_fallar, auditoria, ejecutar_sql, engine
import indicadores as ind_cmf
import indicadores_cne as ind_cne

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": al_fallar,
}


@dag(
    dag_id="indicadores_gestion",
    default_args=DEFAULT_ARGS,
    description="KPIs en SQL sobre dw (mart) y relación entre CNE y CMF",
    schedule=[Asset("dw_cne")],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["indicadores", "mart", "cne", "cmf"],
)
def indicadores_gestion():

    @task()
    def aplicar_ddl():
        ejecutar_sql("ddl.sql")

    @task()
    def cargar_finanzas_cmf():
        """Lleva las cuentas de cada banco desde lo cargado por etl_cmf_indicadores a dw.fact_finanzas_banco."""
        contexto = get_current_context()
        with auditoria(contexto, fuente="cmf", capa="dw") as m:
            motor = engine()
            inspector = sqlalchemy.inspect(motor)
            if not (inspector.has_table("cmf_ind_balances") and inspector.has_table("cmf_ind_resultados")):
                m["mensaje"] = "Sin tablas CMF: se conservan los datos anteriores de dw."
                print(f"Aviso: {m['mensaje']}")
                return
            with motor.connect() as conn:
                balances = pd.read_sql(text("SELECT * FROM public.cmf_ind_balances"), conn)
                resultados = pd.read_sql(text("SELECT * FROM public.cmf_ind_resultados"), conn)
            base = ind_cmf.base_indicadores(balances, resultados)
            m["leidas"] = len(balances) + len(resultados)
            nombres = balances[["CodigoInstitucion", "NombreInstitucion"]].drop_duplicates("CodigoInstitucion")
            filas = [dict(f, id_tiempo=int(f["anio"]) * 10000 + 1231)
                     for f in base.astype(object).where(base.notna(), None).to_dict("records")]
            with motor.begin() as conn:
                for _, n in nombres.iterrows():
                    conn.execute(text("""
                        INSERT INTO dw.dim_banco (id_banco, nombre) VALUES (:i, :n)
                        ON CONFLICT (id_banco) DO UPDATE SET nombre = EXCLUDED.nombre
                    """), {"i": str(n["CodigoInstitucion"]).zfill(3), "n": n["NombreInstitucion"]})
                conn.execute(text("""
                    INSERT INTO dw.fact_finanzas_banco (id_tiempo, id_banco, activos, pasivos, patrimonio, utilidad)
                    VALUES (:id_tiempo, :institucion, :activos, :pasivos, :patrimonio, :utilidad)
                    ON CONFLICT (id_tiempo, id_banco) DO UPDATE SET
                        activos = COALESCE(EXCLUDED.activos, dw.fact_finanzas_banco.activos),
                        pasivos = COALESCE(EXCLUDED.pasivos, dw.fact_finanzas_banco.pasivos),
                        patrimonio = COALESCE(EXCLUDED.patrimonio, dw.fact_finanzas_banco.patrimonio),
                        utilidad = COALESCE(EXCLUDED.utilidad, dw.fact_finanzas_banco.utilidad)
                """), filas)
            m["cargadas"] = len(filas)
            print(f"dw.fact_finanzas_banco: {len(filas)} filas banco-año.")

    @task()
    def calcular_kpis_sql():
        """Ejecuta sql/mart_kpis.sql."""
        contexto = get_current_context()
        with auditoria(contexto, capa="mart") as m:
            ejecutar_sql("mart_kpis.sql")
            with engine().connect() as conn:
                m["cargadas"] = (conn.execute(text("SELECT COUNT(*) FROM mart.kpi_anual")).scalar()
                                 + conn.execute(text("SELECT COUNT(*) FROM mart.kpi_precio_combustible")).scalar())
            print(f"Filas en mart: {m['cargadas']}")

    @task()
    def relacion_cne_cmf():
        """Correlaciones entre KPIs de ambas fuentes, usando el año como campo de cruce."""
        contexto = get_current_context()
        with auditoria(contexto, capa="mart") as m:
            with engine().connect() as conn:
                panel = pd.read_sql(text("SELECT * FROM mart.v_panel_cne_cmf"), conn)
            corr = ind_cne.correlaciones(panel)
            m["leidas"], m["cargadas"] = len(panel), len(corr)
            with engine().begin() as conn:
                conn.execute(text("DELETE FROM mart.relacion_cne_cmf"))
                if not corr.empty:
                    corr.to_sql("relacion_cne_cmf", con=conn, schema="mart", if_exists="append", index=False)
            print(f"Pares calculados: {len(corr)}. El mejor q_valor es {corr['q_valor'].min() if len(corr) else None}.")

    @task()
    def exportar_reportes():
        """Deja una copia en CSV de cada tabla de mart."""
        os.makedirs(REPORTE_DIR, exist_ok=True)
        with engine().connect() as conn:
            for tabla in ("kpi_anual", "kpi_precio_combustible", "participacion_distribuidor", "relacion_cne_cmf"):
                df = pd.read_sql(text(f"SELECT * FROM mart.{tabla}"), conn)
                df.to_csv(os.path.join(REPORTE_DIR, f"mart_{tabla}.csv"), index=False)
                print(f"mart.{tabla}: {len(df)} filas")

    aplicar_ddl() >> cargar_finanzas_cmf() >> calcular_kpis_sql() >> relacion_cne_cmf() >> exportar_reportes()


indicadores_gestion()
