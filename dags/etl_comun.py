# Archivo: dags/etl_comun.py
"""Funciones compartidas por los DAGs: conexión, ejecución de SQL y auditoría."""
from contextlib import contextmanager
from datetime import datetime, timezone
import os
import re

import sqlalchemy
from sqlalchemy import text

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
REPORTE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reportes")


def engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("La variable de entorno DATABASE_URL no está configurada.")
    return sqlalchemy.create_engine(db_url)


def ejecutar_sql(archivo: str, **parametros):
    """Ejecuta un archivo de dags/sql en una sola transacción."""
    with open(os.path.join(SQL_DIR, archivo), encoding="utf-8") as f:
        sql = f.read()
    with engine().begin() as conn:
        conn.execute(text(sql), parametros)


def _limpiar_mensaje(mensaje) -> str:
    """Quita datos sensibles antes de guardar un error en la auditoría."""
    texto = str(mensaje)
    texto = re.sub(r"(apikey|password|token)=[^&\s]+", r"\1=***", texto, flags=re.I)
    return texto[:500]


def registrar_auditoria(dag_id, run_id, task_id, estado, intento=None, fuente=None, capa=None, leidas=None,
                        cargadas=None, rechazadas=None, pct_nulos=None, inicio=None, mensaje=None):
    fin = datetime.now(timezone.utc)
    inicio = inicio or fin
    with engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO audit.etl_audit (dag_id, run_id, task_id, intento, fuente, capa, filas_leidas,
                filas_cargadas, filas_rechazadas, pct_nulos, inicio, fin, duracion_s, estado, mensaje)
            VALUES (:dag_id, :run_id, :task_id, :intento, :fuente, :capa, :leidas, :cargadas, :rechazadas,
                :pct_nulos, :inicio, :fin, :duracion, :estado, :mensaje)
        """), {"dag_id": dag_id, "run_id": run_id, "task_id": task_id, "intento": intento, "fuente": fuente,
               "capa": capa, "leidas": leidas, "cargadas": cargadas, "rechazadas": rechazadas,
               "pct_nulos": pct_nulos, "inicio": inicio, "fin": fin,
               "duracion": (fin - inicio).total_seconds(), "estado": estado,
               "mensaje": _limpiar_mensaje(mensaje) if mensaje else None})


@contextmanager
def auditoria(contexto: dict, fuente=None, capa=None):
    """Registra cada intento de una tarea. Quien la usa completa las métricas en el diccionario."""
    ti = contexto["ti"]
    base = {"dag_id": ti.dag_id, "run_id": ti.run_id, "task_id": ti.task_id, "intento": ti.try_number,
            "fuente": fuente, "capa": capa, "inicio": datetime.now(timezone.utc)}
    metricas = {}
    try:
        yield metricas
    except Exception as e:
        metricas.pop("mensaje", None)
        metricas.pop("estado", None)
        registrar_auditoria(estado="error_intento", mensaje=f"{type(e).__name__}: {e}", **base, **metricas)
        raise
    estado = metricas.pop("estado", "success")  # una tarea puede informar "omitida"
    registrar_auditoria(estado=estado, **base, **metricas)


def al_fallar(contexto: dict):
    """on_failure_callback: deja registro cuando la tarea agotó sus reintentos."""
    ti = contexto["ti"]
    print(f"ALERTA: la tarea {ti.dag_id}.{ti.task_id} falló definitivamente (run {ti.run_id}).")
    registrar_auditoria(ti.dag_id, ti.run_id, ti.task_id, "failed", intento=ti.try_number,
                        mensaje=contexto.get("exception"))
