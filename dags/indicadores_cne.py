# Archivo: dags/indicadores_cne.py
"""
Indicadores de la API CNE y su relación con los indicadores CMF.
No depende de Airflow, así que se puede probar por separado.
Las columnas usadas son las que la API entregó en la ejecución del 2026-11-04.

Series anuales (se cruzan con CMF cuando hay años en común):
  GENERACION_SSMM, PCT_RENOVABLE_SSMM   solo Sistemas Medianos, no todo el país (1998-2021)
  CAPACIDAD_ACUMULADA, PCT_ERNC_CAPACIDAD   unidades hoy en operación, por año de servicio
  IMPORTACIONES, EXPORTACIONES, IMPORTACION_NETA, COBERTURA_EXPORTADORA   en masa (KN)
  VENTAS_COMBUSTIBLE (m3; años completos 2016-2021)
  PIB_EGA, INTENSIDAD_VENTAS_PIB (2013-2019: sin años en común con CMF)

Foto actual de estaciones (sin historia, no se cruza con CMF):
  precio por combustible y región, dispersión y participación de distribuidores (HHI).
"""
from itertools import permutations
import re

import numpy as np
import pandas as pd

from indicadores import parse_numero

RE_RENOVABLE = re.compile(
    r"solar|fotovolt|e[oó]lic|hidr[aá]ulic|pasada|embalse|geot[eé]rm|biomasa|biog[aá]s|mareomotr", re.I)

ESPERADOS = ["GENERACION_SSMM", "PCT_RENOVABLE_SSMM", "CAPACIDAD_ACUMULADA", "PCT_ERNC_CAPACIDAD",
             "VENTAS_COMBUSTIBLE", "IMPORTACIONES", "EXPORTACIONES", "IMPORTACION_NETA",
             "COBERTURA_EXPORTADORA", "PIB_EGA", "INTENSIDAD_VENTAS_PIB"]

# Fuera de este rango respecto de la mediana nacional, un precio se considera erróneo.
# La API trae valores como 99999.999 o 1.000.
PRECIO_MIN_REL, PRECIO_MAX_REL = 0.5, 2.0


# ---------------------------------------------------------------- Utilidades
def _fila_anual(serie: pd.Series, indicador: str, unidad: str) -> pd.DataFrame:
    out = serie.rename("valor").reset_index()
    out.columns = ["anio", "valor"]
    out["anio"] = out["anio"].astype(int)
    out["indicador"] = indicador
    out["unidad"] = unidad
    return out[["anio", "indicador", "valor", "unidad"]]


def _vacio() -> pd.DataFrame:
    return pd.DataFrame(columns=["anio", "indicador", "valor", "unidad"])


def _unir(partes) -> pd.DataFrame:
    return pd.concat([p for p in partes if not p.empty] or [_vacio()], ignore_index=True)


def _preparar(df, indicador, *columnas):
    """Agrega _anio, _mes y _valor (la primera columna pedida). None si falta alguna."""
    if df is None or df.empty:
        print(f"Aviso: {indicador} omitido; la fuente no tiene datos.")
        return None
    faltan = [c for c in ("anio", *columnas) if c not in df.columns]
    if faltan:
        print(f"Aviso: {indicador} omitido; faltan las columnas {faltan}. Columnas: {list(df.columns)}")
        return None
    df = df.copy()
    df["_anio"] = pd.to_numeric(df["anio"], errors="coerce")
    df["_mes"] = pd.to_numeric(df["mes"], errors="coerce") if "mes" in df.columns else float("nan")
    df["_valor"] = df[columnas[0]].map(parse_numero)
    return df.dropna(subset=["_anio"])  # " - " cuenta el mes como informado; la suma ignora el NaN


def _anios_completos(df: pd.DataFrame, indicador: str) -> pd.DataFrame:
    """Un flujo anual solo es comparable con 12 meses: se excluyen los años parciales."""
    if df["_mes"].notna().sum() == 0:
        return df
    meses = df.groupby("_anio")["_mes"].nunique()
    parciales = sorted(int(a) for a in meses[meses < 12].index)
    if parciales:
        print(f"Aviso: {indicador} excluye años incompletos: {parciales}.")
    return df[df["_anio"].isin(meses[meses == 12].index)]


def flujo_anual(df, indicador, columna, unidad, factor=1.0) -> pd.DataFrame:
    """Suma anual de una columna de flujo, usando solo años completos."""
    datos = _preparar(df, indicador, columna)
    if datos is None:
        return _vacio()
    datos = _anios_completos(datos, indicador)
    if datos.empty:
        return _vacio()
    return _fila_anual(datos.groupby("_anio")["_valor"].sum() * factor, indicador, unidad)


def _solo_unidad(df, indicador, unidad):
    """No se suman unidades distintas: se conserva solo la indicada."""
    if df is None or df.empty or "unidad_medida" not in df.columns:
        return df
    otras = df.loc[df["unidad_medida"] != unidad, "unidad_medida"].unique().tolist()
    if otras:
        print(f"Aviso: {indicador} descarta filas con unidades {otras}; se usa solo {unidad}.")
    return df[df["unidad_medida"] == unidad]


# ------------------------------------------------------------- Indicadores anuales
def es_renovable(tecnologia) -> bool:
    return bool(RE_RENOVABLE.search(str(tecnologia)))


def kpi_generacion(generacion) -> pd.DataFrame:
    """Generación bruta y % renovable (hidro + ERNC). El endpoint solo cubre Sistemas Medianos."""
    total = flujo_anual(generacion, "GENERACION_SSMM", "generacion_kwh", "GWh", factor=1e-6)
    datos = _preparar(generacion, "PCT_RENOVABLE_SSMM", "generacion_kwh", "tecnologia")
    if datos is None:
        return total
    datos = _anios_completos(datos, "PCT_RENOVABLE_SSMM")
    renovable = datos[datos["tecnologia"].map(es_renovable)].groupby("_anio")["_valor"].sum()
    suma = datos.groupby("_anio")["_valor"].sum()
    pct = (renovable.reindex(suma.index).fillna(0) / suma.where(suma != 0)) * 100
    return _unir([total, _fila_anual(pct.dropna(), "PCT_RENOVABLE_SSMM", "%")])


def kpi_capacidad(capacidad) -> pd.DataFrame:
    """
    La API entrega solo la foto actual del parque. La serie acumula las unidades hoy en operación
    según su año de servicio; no incluye unidades ya retiradas, así que subestima años antiguos.
    """
    requeridas = ["anio_servicio_central", "potencia_neta_mw", "estado", "clasificacion"]
    if capacidad is None or capacidad.empty or any(c not in capacidad.columns for c in requeridas):
        print("Aviso: CAPACIDAD_ACUMULADA omitido; faltan datos o columnas.")
        return _vacio()
    df = capacidad[capacidad["estado"].astype(str).str.strip().str.lower() == "en operacion"].copy()
    df["_anio"] = pd.to_numeric(df["anio_servicio_central"], errors="coerce")
    df["_mw"] = df["potencia_neta_mw"].map(parse_numero)
    sin_dato = df["_anio"].isna() | df["_mw"].isna()
    if sin_dato.any():
        print(f"Aviso: CAPACIDAD_ACUMULADA excluye {int(sin_dato.sum())} unidades sin año o potencia.")
    df = df[~sin_dato]
    if df.empty:
        return _vacio()
    anios = range(int(df["_anio"].min()), int(df["_anio"].max()) + 1)
    total = df.groupby("_anio")["_mw"].sum().reindex(anios, fill_value=0).cumsum()
    ernc = (df[df["clasificacion"].astype(str).str.strip() == "ERNC"].groupby("_anio")["_mw"].sum()
            .reindex(anios, fill_value=0).cumsum())
    pct = (ernc / total.where(total != 0)) * 100
    return _unir([_fila_anual(total, "CAPACIDAD_ACUMULADA", "MW"),
                  _fila_anual(pct.dropna(), "PCT_ERNC_CAPACIDAD", "%")])


def kpi_mercado(ventas, importacion, exportacion, pib) -> pd.DataFrame:
    v = flujo_anual(ventas, "VENTAS_COMBUSTIBLE", "volumen_m3", "m3")
    i = flujo_anual(_solo_unidad(importacion, "IMPORTACIONES", "KN"), "IMPORTACIONES", "cantidad_mercancias", "KN")
    e = flujo_anual(_solo_unidad(exportacion, "EXPORTACIONES", "KN"), "EXPORTACIONES", "cantidad_mercancias", "KN")
    p = flujo_anual(pib, "PIB_EGA", "pib_regional_ega", "según API")
    salida = [v, i, e, p]

    if not i.empty and not e.empty:
        cruce = i.merge(e, on="anio", suffixes=("_i", "_e")).set_index("anio")
        salida.append(_fila_anual(cruce["valor_i"] - cruce["valor_e"], "IMPORTACION_NETA", "KN"))
        cobertura = cruce["valor_e"] / cruce["valor_i"].replace(0, np.nan) * 100
        salida.append(_fila_anual(cobertura.dropna(), "COBERTURA_EXPORTADORA", "%"))

    if not v.empty and not p.empty:
        cruce = v.merge(p, on="anio", suffixes=("_v", "_p")).set_index("anio")
        intensidad = cruce["valor_v"] / cruce["valor_p"].replace(0, np.nan)
        salida.append(_fila_anual(intensidad.dropna(), "INTENSIDAD_VENTAS_PIB", "ratio"))

    return _unir(salida)


def consolidar_anual(*kpis) -> pd.DataFrame:
    """Une las series y agrega la variación anual, con el mismo formato que los indicadores CMF."""
    df = _unir(kpis)
    if df.empty:
        return df.assign(variacion_anual=pd.Series(dtype=float))
    df["anio"] = df["anio"].astype(int)
    df["valor"] = df["valor"].astype(float).round(4)
    df = df.sort_values(["indicador", "anio"]).reset_index(drop=True)
    df["variacion_anual"] = df.groupby("indicador")["valor"].diff().round(4)
    return df


# ------------------------------------------------------------- Estaciones (foto actual)
def _campo(d, palabra):
    """Valor de la primera clave que contiene la palabra, prefiriendo nombre sobre código."""
    if not isinstance(d, dict):
        return None
    claves = [k for k in d if palabra in str(k).lower()]
    claves.sort(key=lambda k: 0 if "nom" in str(k).lower() else 1)
    return d[claves[0]] if claves else None


def aplanar_estaciones(payload) -> pd.DataFrame:
    """De la respuesta anidada de /v4/estaciones a una fila por estación y combustible."""
    if isinstance(payload, dict):
        payload = payload.get("data") if isinstance(payload.get("data"), list) else list(payload.values())
    filas = []
    for est in payload or []:
        if not isinstance(est, dict):
            continue
        ubic = est.get("ubicacion") if isinstance(est.get("ubicacion"), dict) else {}
        dist = est.get("distribuidor") if isinstance(est.get("distribuidor"), dict) else {}
        precios = est.get("precios") if isinstance(est.get("precios"), dict) else {}
        for combustible, oferta in precios.items():
            if not isinstance(oferta, dict):
                continue
            filas.append({
                "codigo": est.get("codigo"),
                "marca": dist.get("marca") or "Sin marca",
                "region": _campo(ubic, "region") or _campo(est, "region") or "Sin región",
                "comuna": _campo(ubic, "comuna") or _campo(est, "comuna") or "Sin comuna",
                "latitud": ubic.get("latitud"),
                "longitud": ubic.get("longitud"),
                "combustible": str(combustible),
                "precio": oferta.get("precio"),
                "tipo_atencion": oferta.get("tipo_atencion"),
                "unidad": oferta.get("unidad_cobro"),
                "fecha_actualizacion": oferta.get("fecha_actualizacion"),
            })
    return pd.DataFrame(filas)


def kpi_precios(estaciones, dias_max=90, hoy=None) -> pd.DataFrame:
    """Precio por combustible y región (más el total NACIONAL), descartando precios antiguos."""
    columnas = ["combustible", "unidad", "region", "n_estaciones", "precio_promedio", "precio_mediana",
                "precio_min", "precio_max", "cv_pct", "brecha_pct"]
    if estaciones is None or estaciones.empty:
        return pd.DataFrame(columns=columnas)
    df = estaciones.copy()
    # La API usa punto decimal ("625.000" son 625 pesos), no el formato chileno de CMF
    df["precio"] = pd.to_numeric(df["precio"], errors="coerce")
    df = df[df["precio"] > 0]
    mediana = df.groupby("combustible")["precio"].transform("median")
    atipicos = (df["precio"] < mediana * PRECIO_MIN_REL) | (df["precio"] > mediana * PRECIO_MAX_REL)
    if atipicos.any():
        print(f"Aviso: se descartan {int(atipicos.sum())} precios atípicos: "
              f"{df.loc[atipicos, ['combustible', 'precio']].values.tolist()}")
    df = df[~atipicos]
    hoy = pd.Timestamp(hoy) if hoy is not None else pd.Timestamp.now()
    fecha = pd.to_datetime(df["fecha_actualizacion"], errors="coerce")
    antiguos = fecha.notna() & ((hoy - fecha).dt.days > dias_max)
    if antiguos.any():
        print(f"Aviso: se descartan {int(antiguos.sum())} precios con más de {dias_max} días.")
    df = df[~antiguos].assign(unidad=lambda d: d["unidad"].fillna("según API").astype(str))
    if df.empty:
        return pd.DataFrame(columns=columnas)

    def resumen(grupo):
        out = grupo.groupby(["combustible", "unidad", "region"])["precio"].agg(
            n_estaciones="count", precio_promedio="mean", precio_mediana="median",
            precio_min="min", precio_max="max", desv="std").reset_index()
        out["cv_pct"] = out["desv"] / out["precio_promedio"] * 100
        out["brecha_pct"] = (out["precio_max"] - out["precio_min"]) / out["precio_min"] * 100
        return out.drop(columns="desv")

    tabla = pd.concat([resumen(df), resumen(df.assign(region="NACIONAL"))], ignore_index=True)
    return tabla.round(2)[columnas].sort_values(["combustible", "region"]).reset_index(drop=True)


def kpi_distribuidores(estaciones) -> pd.DataFrame:
    """Participación de cada marca por número de estaciones y concentración de mercado (HHI)."""
    if estaciones is None or estaciones.empty:
        return pd.DataFrame(columns=["marca", "n_estaciones", "participacion_pct", "hhi_mercado"])
    unicas = estaciones.drop_duplicates(subset=["codigo"])
    out = unicas.groupby("marca").size().rename("n_estaciones").reset_index()
    out["participacion_pct"] = out["n_estaciones"] / out["n_estaciones"].sum() * 100
    out["hhi_mercado"] = (out["participacion_pct"] ** 2).sum()
    return out.round(2).sort_values("n_estaciones", ascending=False).reset_index(drop=True)


# ----------------------------------------------------------------- Relación CNE - CMF
def _rangos(a):
    return pd.Series(a).rank().to_numpy()


def _p_permutacion(x, y, semilla=7):
    """p bilateral de Pearson por permutaciones: válido con pocos años, sin supuesto de normalidad."""
    n = len(x)
    xz = x - x.mean()
    yz = y - y.mean()
    denom = np.sqrt((xz ** 2).sum() * (yz ** 2).sum())
    if denom == 0:
        return float("nan"), float("nan")
    r = float(xz @ yz / denom)
    if n <= 7:
        perms = np.array(list(permutations(range(n))))
    else:
        perms = np.array([np.random.default_rng(semilla + i).permutation(n) for i in range(5000)])
    distrib = (yz[perms] @ xz) / denom
    return r, float((np.abs(distrib) >= abs(r) - 1e-12).mean())


def _q_bh(pvalores: pd.Series) -> pd.Series:
    """Ajuste Benjamini-Hochberg: se prueban muchos pares y algunos saldrían altos por azar."""
    p = pvalores.dropna().sort_values()
    if p.empty:
        return pd.Series(np.nan, index=pvalores.index)
    q = (p * len(p) / np.arange(1, len(p) + 1))[::-1].cummin()[::-1].clip(upper=1)
    return q.reindex(pvalores.index)


def _cmf_con_sistema(cmf: pd.DataFrame) -> pd.DataFrame:
    df = cmf[["institucion", "anio", "indicador", "valor"]].copy()
    df["institucion"] = df["institucion"].astype(str).str.zfill(3)
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype(int)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    sistema = df.groupby(["anio", "indicador"], as_index=False)["valor"].mean().assign(institucion="SISTEMA")
    return pd.concat([df, sistema], ignore_index=True)


def panel_cne_cmf(cmf: pd.DataFrame, cne: pd.DataFrame) -> pd.DataFrame:
    """Tabla banco-año-indicador CMF frente a cada indicador nacional CNE del mismo año."""
    a = _cmf_con_sistema(cmf).rename(columns={"indicador": "indicador_cmf", "valor": "valor_cmf"})
    b = cne[["anio", "indicador", "valor"]].rename(columns={"indicador": "indicador_cne", "valor": "valor_cne"})
    b = b.assign(anio=b["anio"].astype(int))
    panel = a.merge(b, on="anio", how="inner")
    return panel.sort_values(["institucion", "indicador_cmf", "indicador_cne", "anio"]).reset_index(drop=True)


def correlaciones(panel: pd.DataFrame, n_min=4) -> pd.DataFrame:
    """
    Correlaciones por banco, indicador CMF e indicador CNE, en dos formas:
      nivel:     valores anuales; sensible a tendencias comunes.
      variacion: cambios año a año; quita esas tendencias, pero con un punto menos.
    Son asociaciones exploratorias entre pocos años: no prueban causalidad.
    """
    columnas = ["institucion", "indicador_cmf", "indicador_cne", "tipo", "n", "anio_desde", "anio_hasta",
                "pearson", "spearman", "p_valor", "q_valor", "lectura"]
    filas = []
    for llaves, g in panel.groupby(["institucion", "indicador_cmf", "indicador_cne"]):
        g = g.dropna(subset=["valor_cmf", "valor_cne"]).sort_values("anio")
        consecutivos = g["anio"].diff() == 1
        variacion = pd.DataFrame({"x": g["valor_cmf"].diff(), "y": g["valor_cne"].diff(),
                                  "anio": g["anio"]})[consecutivos]
        nivel = pd.DataFrame({"x": g["valor_cmf"], "y": g["valor_cne"], "anio": g["anio"]})
        for tipo, datos in (("nivel", nivel), ("variacion", variacion)):
            x, y = datos["x"].to_numpy(float), datos["y"].to_numpy(float)
            if len(x) < n_min or x.std() == 0 or y.std() == 0:
                continue
            r, p = _p_permutacion(x, y)
            rho = float(np.corrcoef(_rangos(x), _rangos(y))[0, 1])
            filas.append({"institucion": llaves[0], "indicador_cmf": llaves[1], "indicador_cne": llaves[2],
                          "tipo": tipo, "n": len(x), "anio_desde": int(datos["anio"].min()),
                          "anio_hasta": int(datos["anio"].max()), "pearson": r, "spearman": rho, "p_valor": p})
    if not filas:
        return pd.DataFrame(columns=columnas)
    df = pd.DataFrame(filas)
    df["q_valor"] = _q_bh(df["p_valor"])

    def lectura(f):
        signo = "positiva" if f["pearson"] > 0 else "negativa"
        if f["q_valor"] < 0.10:
            return f"asociación {signo} (exploratoria)"
        if abs(f["pearson"]) >= 0.7:
            return f"indicio {signo}, no concluyente"
        return "sin relación clara"

    df["lectura"] = df.apply(lectura, axis=1)
    df[["pearson", "spearman", "p_valor", "q_valor"]] = df[["pearson", "spearman", "p_valor", "q_valor"]].round(4)
    return df[columnas].sort_values(["q_valor", "p_valor"]).reset_index(drop=True)
