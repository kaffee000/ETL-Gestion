# Archivo: dags/transform_cne.py
"""
Limpieza de las respuestas de la API CNE antes de pasar a staging.
No depende de Airflow, así que se puede probar por separado.

Cada función recibe la lista de respuestas crudas y devuelve un Resultado con la tabla limpia y sus
métricas de calidad. Si faltan columnas lanza un error: un cambio en la API no debe pasar en silencio.
"""
from dataclasses import dataclass
import re
import unicodedata

import pandas as pd

from indicadores import parse_numero

# Fuera de este rango respecto de la mediana nacional, un precio se considera erróneo.
# La API trae valores como 99999.999 o 1.000.
PRECIO_MIN_REL, PRECIO_MAX_REL = 0.5, 2.0
PRECIO_DIAS_MAX = 90

# Mapeo de nombres de región, que varían entre endpoints, a su código oficial
REGIONES = [("arica", 15), ("tarapac", 1), ("antofag", 2), ("atacama", 3), ("coquimbo", 4), ("valpara", 5),
            ("metropolitana", 13), ("higgins", 6), ("maule", 7), ("nuble", 16), ("biobio", 8), ("araucan", 9),
            ("los rios", 14), ("los lagos", 10), ("ays", 11), ("ais", 11), ("magallanes", 12)]

TECNOLOGIAS = {"petroleo diesel": "Petróleo Diésel", "hidraulica de pasada": "Hidráulica de Pasada",
               "hidraulica pasada": "Hidráulica de Pasada", "mini hidraulica pasada": "Mini Hidráulica de Pasada",
               "mini hidraulica de pasada": "Mini Hidráulica de Pasada",
               "hidraulica embalse": "Hidráulica de Embalse", "hidraulica de embalse": "Hidráulica de Embalse",
               "solar fotovoltaica": "Solar Fotovoltaica", "solar": "Solar Fotovoltaica"}


@dataclass
class Resultado:
    tabla: pd.DataFrame
    leidas: int
    rechazadas: int
    pct_nulos: float
    detalle: str = ""


def _norm(texto) -> str:
    s = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().lower()


def codigo_region(nombre) -> int:
    n = _norm(nombre)
    for clave, codigo in REGIONES:
        if clave in n:
            return codigo
    return 0


def tecnologia_canonica(nombre) -> str:
    return TECNOLOGIAS.get(_norm(nombre), re.sub(r"\s+", " ", str(nombre)).strip())


def a_tabla(payload) -> pd.DataFrame:
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


def contar_filas(payload) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for v in payload.values():
            if isinstance(v, list):
                return len(v)
        return len(payload)
    return 0


def _unir(respuestas) -> pd.DataFrame:
    partes = [p for p in map(a_tabla, respuestas) if not p.empty]
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def _validar(df: pd.DataFrame, columnas, fuente: str):
    if df.empty:
        raise ValueError(f"{fuente}: la API no devolvió filas.")
    faltan = [c for c in columnas if c not in df.columns]
    if faltan:
        raise ValueError(f"{fuente}: cambió la estructura; faltan {faltan}. Columnas: {list(df.columns)}")


def _pct_nulos(df: pd.DataFrame, columnas) -> float:
    vacios = df[columnas].astype("object").map(lambda v: v is None or pd.isna(v) or str(v).strip() in ("", "-"))
    return round(float(vacios.to_numpy().mean()) * 100, 2)


def _entero(serie):
    return pd.to_numeric(serie, errors="coerce")


def limpiar_regiones(respuestas) -> Resultado:
    df = _unir(respuestas)
    _validar(df, ["cod_region", "nom_region"], "regiones")
    tabla = pd.DataFrame({"id_region": _entero(df["cod_region"]), "nombre": df["nom_region"].astype(str).str.strip()})
    tabla = tabla.dropna(subset=["id_region"]).drop_duplicates("id_region")
    return Resultado(tabla.astype({"id_region": int}), len(df), len(df) - len(tabla), _pct_nulos(df, ["cod_region", "nom_region"]))


def limpiar_generacion(respuestas) -> Resultado:
    columnas = ["anio", "mes", "central", "tecnologia", "clasificacion", "sistema", "cut_region", "generacion_kwh"]
    df = _unir(respuestas)
    _validar(df, columnas, "generacion")
    leidas, nulos = len(df), _pct_nulos(df, columnas)
    df = df.drop_duplicates()
    tabla = pd.DataFrame({
        "anio": _entero(df["anio"]), "mes": _entero(df["mes"]), "central": df["central"],
        "tecnologia": df["tecnologia"].map(tecnologia_canonica),
        "clasificacion": df["clasificacion"].astype(str).str.strip(),
        "sistema": df["sistema"].astype(str).str.strip(),
        "id_region": _entero(df["cut_region"]).fillna(0).astype(int),
        "generacion_kwh": df["generacion_kwh"].map(parse_numero)})
    tabla = tabla.dropna(subset=["anio", "mes"])
    tabla = tabla[tabla["mes"].between(1, 12) & ~(tabla["generacion_kwh"] < 0)]
    return Resultado(tabla.astype({"anio": int, "mes": int}), leidas, leidas - len(tabla), nulos)


def limpiar_capacidad(respuestas) -> Resultado:
    """Solo unidades en operación y con año y potencia conocidos."""
    columnas = ["central", "cod_unidad", "sistema", "estado", "anio_servicio_central", "region_nombre",
                "tipo_de_energia", "clasificacion", "potencia_neta_mw"]
    df = _unir(respuestas)
    _validar(df, columnas, "capacidad")
    leidas, nulos = len(df), _pct_nulos(df, ["estado", "anio_servicio_central", "potencia_neta_mw", "clasificacion"])
    df = df.drop_duplicates()
    fuera = int((df["estado"].astype(str).str.strip().str.lower() != "en operacion").sum())
    df = df[df["estado"].astype(str).str.strip().str.lower() == "en operacion"]
    tabla = pd.DataFrame({
        "central": df["central"], "cod_unidad": df["cod_unidad"], "sistema": df["sistema"],
        "id_region": df["region_nombre"].map(codigo_region),
        "tecnologia": df["tipo_de_energia"].map(tecnologia_canonica),
        "clasificacion": df["clasificacion"].astype(str).str.strip(),
        "anio_servicio": _entero(df["anio_servicio_central"]),
        "potencia_neta_mw": df["potencia_neta_mw"].map(parse_numero)})
    tabla = tabla.dropna(subset=["anio_servicio", "potencia_neta_mw"])
    return Resultado(tabla.astype({"anio_servicio": int}), leidas, leidas - len(tabla), nulos,
                     f"{fuera} unidades no están en operación")


def limpiar_ventas(respuestas) -> Resultado:
    """Un volumen ' - ' queda nulo, pero la fila se conserva: el mes sí fue informado."""
    columnas = ["anio", "mes", "region_cod", "tipo_combustible", "volumen_m3"]
    df = _unir(respuestas)
    _validar(df, columnas, "ventas")
    leidas, nulos = len(df), _pct_nulos(df, columnas)
    df = df.drop_duplicates()
    tabla = pd.DataFrame({
        "anio": _entero(df["anio"]), "mes": _entero(df["mes"]),
        "id_region": _entero(df["region_cod"]).fillna(0).astype(int),
        "producto": df["tipo_combustible"].astype(str).str.strip(),
        "volumen_m3": df["volumen_m3"].map(parse_numero)})
    tabla = tabla.dropna(subset=["anio", "mes"])
    return Resultado(tabla.astype({"anio": int, "mes": int}), leidas, leidas - len(tabla), nulos)


def limpiar_comercio(respuestas, flujo: str) -> Resultado:
    """Importación ('I') o exportación ('E'). Solo se conserva la unidad KN para no mezclar unidades."""
    columnas = ["anio", "mes", "tipo_arancel_reporte", "cif", "cantidad_mercancias", "unidad_medida"]
    df = _unir(respuestas)
    _validar(df, columnas, "comercio")
    leidas, nulos = len(df), _pct_nulos(df, columnas)
    df = df.drop_duplicates()
    df = df[df["unidad_medida"].astype(str).str.strip() == "KN"]
    tabla = pd.DataFrame({
        "flujo": flujo, "anio": _entero(df["anio"]), "mes": _entero(df["mes"]),
        "producto": df["tipo_arancel_reporte"].astype(str).str.strip(),
        "cantidad_kn": df["cantidad_mercancias"].map(parse_numero), "cif": df["cif"].map(parse_numero)})
    tabla = tabla.dropna(subset=["anio", "mes"])
    return Resultado(tabla.astype({"anio": int, "mes": int}), leidas, leidas - len(tabla), nulos)


def limpiar_pib(respuestas) -> Resultado:
    columnas = ["anio", "region_cod", "pib_regional_ega"]
    df = _unir(respuestas)
    _validar(df, columnas, "pib_regional")
    leidas, nulos = len(df), _pct_nulos(df, columnas)
    df = df.drop_duplicates()
    tabla = pd.DataFrame({"anio": _entero(df["anio"]), "id_region": _entero(df["region_cod"]).fillna(0).astype(int),
                          "pib_ega": df["pib_regional_ega"].map(parse_numero)}).dropna(subset=["anio"])
    return Resultado(tabla.astype({"anio": int}), leidas, leidas - len(tabla), nulos)


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
                "codigo_estacion": est.get("codigo"),
                "marca": str(dist.get("marca") or "Sin marca").strip(),
                "region": _campo(ubic, "region") or _campo(est, "region") or "Sin región",
                "comuna": _campo(ubic, "comuna") or _campo(est, "comuna"),
                "combustible": str(combustible),
                "precio": oferta.get("precio"),
                "tipo_atencion": oferta.get("tipo_atencion"),
                "unidad": oferta.get("unidad_cobro"),
                "fecha_actualizacion": oferta.get("fecha_actualizacion"),
            })
    return pd.DataFrame(filas)


def limpiar_estaciones(respuestas, fecha_snapshot) -> Resultado:
    """Una fila por estación y combustible. Descarta precios inválidos, atípicos o antiguos."""
    partes = [p for p in map(aplanar_estaciones, respuestas) if not p.empty]
    df = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
    columnas = ["codigo_estacion", "marca", "region", "combustible", "precio", "fecha_actualizacion"]
    _validar(df, columnas, "estaciones")
    leidas, nulos = len(df), _pct_nulos(df, columnas)
    hoy = pd.Timestamp(fecha_snapshot)

    # La API usa punto decimal ("625.000" son 625 pesos), no el formato chileno de CMF
    df["precio"] = pd.to_numeric(df["precio"], errors="coerce")
    invalidos = ~(df["precio"] > 0)
    fechas = pd.to_datetime(df["fecha_actualizacion"], errors="coerce")
    antiguos = fechas.notna() & ((hoy - fechas).dt.days > PRECIO_DIAS_MAX)
    mediana = df[~invalidos].groupby("combustible")["precio"].transform("median").reindex(df.index)
    atipicos = (df["precio"] < mediana * PRECIO_MIN_REL) | (df["precio"] > mediana * PRECIO_MAX_REL)

    limpio = df[~(invalidos | antiguos | atipicos)].drop_duplicates(["codigo_estacion", "combustible"])
    tabla = pd.DataFrame({
        "fecha_snapshot": hoy.date(), "codigo_estacion": limpio["codigo_estacion"].astype(str), "marca": limpio["marca"],
        "id_region": limpio["region"].map(codigo_region), "comuna": limpio["comuna"],
        "combustible": limpio["combustible"], "tipo_atencion": limpio["tipo_atencion"], "unidad": limpio["unidad"],
        "precio": limpio["precio"], "fecha_actualizacion": pd.to_datetime(limpio["fecha_actualizacion"], errors="coerce").dt.date})
    detalle = (f"{int(invalidos.sum())} sin precio válido, {int(antiguos.sum())} con más de {PRECIO_DIAS_MAX} días, "
               f"{int(atipicos.sum())} atípicos: {df.loc[atipicos, ['combustible', 'precio']].values.tolist()}")
    return Resultado(tabla, leidas, leidas - len(tabla), nulos, detalle)
