# Archivo: dags/indicadores.py
"""
Cálculo de los 3 indicadores de gestión a partir de las tablas cmf_balances
y cmf_resultados. No depende de Airflow, así que se puede probar con pytest.

Indicadores (por banco y año, con el cierre del último mes disponible):
  1. ROA  = Utilidad del ejercicio / Activos totales        (rentabilidad de los activos)
  2. ROE  = Utilidad del ejercicio / Patrimonio total       (rentabilidad para los accionistas)
  3. Apalancamiento = Pasivos totales / Patrimonio total    (solvencia: veces que la deuda cubre al capital)
"""
import re
import pandas as pd

# Cuentas contables usadas. Se busca primero por código y, si no aparece,
# por descripción (expresión regular). Ajusta los códigos si la CMF cambia su plan de cuentas.
CUENTAS = {
    # Códigos reales de la API CMF. Desde 2022 cambian a 9 dígitos; activos, pasivos y
    # patrimonio se reconocen por descripción ("TOTAL ACTIVOS", "TOTAL PASIVOS", "PATRIMONIO").
    "activos":    {"codigos": ["1000000"], "patron": r"^(?:total\s+)?activos$"},
    "pasivos":    {"codigos": ["2000000"], "patron": r"^(?:total\s+)?pasivos$"},
    "patrimonio": {"codigos": ["3000000"], "patron": r"^(?:total\s+)?patrimonio$"},
    # 2020-2021: 5010000 "UTILIDAD (PERDIDA) CONSOLIDADA DEL EJERCICIO"
    # 2022+ (nuevo plan de cuentas): 590000000 "UTILIDAD (PÉRDIDA) DEL EJERCICIO (O PERIODO)"
    "utilidad":   {"codigos": ["5010000", "590000000"],
                   "patron": r"^utilidad\s*\(p[eé]rdida\)\s*(?:consolidada\s+)?del\s+ejercicio"},
}

FUENTE_CUENTA = {"activos": "balances", "pasivos": "balances",
                 "patrimonio": "balances", "utilidad": "resultados"}


def parse_numero(valor):
    """Convierte '1.234.567,89', '1234567.89' o 1234567 en float. Vacío -> NaN."""
    if valor is None:
        return float("nan")
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).strip().replace(" ", "")
    if s == "":
        return float("nan")
    if "," in s:                       # formato chileno: punto miles, coma decimal
        s = s.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"-?\d{1,3}(\.\d{3})+", s):  # solo puntos de miles: 600.000
        s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _col(df, *candidatos):
    """Devuelve la primera columna cuyo nombre (sin mayúsculas) contiene algún candidato."""
    cols = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        for low, real in cols.items():
            if cand in low:
                return real
    return None


def extraer_cuentas(df: pd.DataFrame, cuentas: dict) -> pd.DataFrame:
    """
    De una tabla cruda (balances o resultados) obtiene una fila por banco-año
    con el valor de cada cuenta pedida, tomando el último mes disponible del año.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["institucion", "anio", *cuentas])

    df = df.copy()
    c_codigo = _col(df, "codigocuenta", "codigo_cuenta", "cuenta")
    c_desc = _col(df, "descripcioncuenta", "descripcion", "nombrecuenta")
    c_valor = _col(df, "monedatotal", "montototal", "total", "monto", "valor")
    c_mes = "mes" if "mes" in df.columns else _col(df, "mes")
    if c_valor is None:
        raise ValueError(f"No se encontró columna de monto en: {list(df.columns)}")

    df["_codigo"] = df[c_codigo].astype(str).str.strip() if c_codigo else ""
    df["_desc"] = df[c_desc].astype(str).str.strip().str.lower() if c_desc else ""
    df["_valor"] = df[c_valor].map(parse_numero)
    df["_mes"] = pd.to_numeric(df[c_mes], errors="coerce").fillna(12) if c_mes else 12
    df["institucion"] = df["institucion"].astype(str).str.zfill(3)
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")

    # Último mes disponible por banco y año (cierre del período)
    ultimo = df.groupby(["institucion", "anio"])["_mes"].transform("max")
    df = df[df["_mes"] == ultimo]

    salida = df[["institucion", "anio"]].drop_duplicates().set_index(["institucion", "anio"])
    for nombre, regla in cuentas.items():
        # Prioridad 0 = coincide el código; 1 = coincide la descripción (respaldo por banco-año)
        prioridad = pd.Series(pd.NA, index=df.index, dtype="Int64")
        prioridad[df["_desc"].str.contains(regla["patron"], flags=re.I, regex=True)] = 1
        prioridad[df["_codigo"].isin(regla["codigos"])] = 0
        candidatas = df.assign(_prio=prioridad).dropna(subset=["_prio"])
        serie = (candidatas.sort_values("_prio")
                 .groupby(["institucion", "anio"])["_valor"].first())
        salida[nombre] = serie
    return salida.reset_index()


def base_indicadores(balances: pd.DataFrame, resultados: pd.DataFrame) -> pd.DataFrame:
    """Une las cuentas de balance y resultados en una sola tabla banco-año."""
    cb = {k: v for k, v in CUENTAS.items() if FUENTE_CUENTA[k] == "balances"}
    cr = {k: v for k, v in CUENTAS.items() if FUENTE_CUENTA[k] == "resultados"}
    b = extraer_cuentas(balances, cb)
    r = extraer_cuentas(resultados, cr)
    base = b.merge(r, on=["institucion", "anio"], how="outer")
    return base.sort_values(["institucion", "anio"]).reset_index(drop=True)


def _ratio(num, den):
    den = den.where(den != 0)
    return num / den


def kpi_roa(base: pd.DataFrame) -> pd.DataFrame:
    out = base[["institucion", "anio"]].copy()
    out["indicador"] = "ROA"
    out["valor"] = _ratio(base["utilidad"], base["activos"]) * 100
    out["unidad"] = "%"
    return out


def kpi_roe(base: pd.DataFrame) -> pd.DataFrame:
    out = base[["institucion", "anio"]].copy()
    out["indicador"] = "ROE"
    out["valor"] = _ratio(base["utilidad"], base["patrimonio"]) * 100
    out["unidad"] = "%"
    return out


def kpi_apalancamiento(base: pd.DataFrame) -> pd.DataFrame:
    out = base[["institucion", "anio"]].copy()
    out["indicador"] = "APALANCAMIENTO"
    out["valor"] = _ratio(base["pasivos"], base["patrimonio"])
    out["unidad"] = "veces"
    return out


# Semáforo de referencia para la banca chilena (ajustable)
UMBRALES = {
    #                 rojo si <      verde si >=     (para apalancamiento es al revés)
    "ROA":            {"malo": 0.5, "bueno": 1.0, "mayor_es_mejor": True},
    "ROE":            {"malo": 8.0, "bueno": 12.0, "mayor_es_mejor": True},
    "APALANCAMIENTO": {"malo": 14.0, "bueno": 11.0, "mayor_es_mejor": False},
}


def semaforo(indicador, valor):
    if pd.isna(valor):
        return "sin_dato"
    u = UMBRALES[indicador]
    if u["mayor_es_mejor"]:
        return "verde" if valor >= u["bueno"] else ("rojo" if valor < u["malo"] else "amarillo")
    return "verde" if valor <= u["bueno"] else ("rojo" if valor > u["malo"] else "amarillo")


def consolidar(*kpis: pd.DataFrame) -> pd.DataFrame:
    """Une los indicadores, agrega variación anual y semáforo."""
    df = pd.concat(kpis, ignore_index=True)
    df = df.sort_values(["indicador", "institucion", "anio"]).reset_index(drop=True)
    df["valor"] = df["valor"].astype(float).round(4)
    df["variacion_anual"] = (
        df.groupby(["indicador", "institucion"])["valor"].diff().round(4)
    )
    df["semaforo"] = [semaforo(i, v) for i, v in zip(df["indicador"], df["valor"])]
    return df
