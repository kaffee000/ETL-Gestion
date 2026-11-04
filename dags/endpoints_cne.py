# Archivo: dags/endpoints_cne.py
# Endpoints de la API CNE que consulta el DAG etl_cne_indicadores.
# Cada config puede tener varias URL: sus respuestas se unen en una sola tabla.

API_BASE = "https://api.cne.cl/api"
URL_LOGIN = f"{API_BASE}/login"

CODIGOS_REGIONES = [f"{r:02d}" for r in range(1, 17)]


def _config(tipo: str, *rutas: str) -> dict:
    return {"id_fuente": tipo, "tipo": tipo, "urls": [f"{API_BASE}/{r}" for r in rutas]}


API_CONFIGS = [
    # 1. Precios y retail de combustibles
    _config("estaciones", "v4/estaciones"),
    _config("distribuidores", "v4/combustible/vehicular/distribuidores"),
    _config("tipos_combustibles", "v4/combustible/vehicular/tiposcombustibles"),
    # 2. Matriz y generación eléctrica
    _config("generacion", "ea/gx/brutassmmmensual"),
    _config("capacidad", "ea/capacidad/instaladagx"),
    _config("dicc_generacion", "ea/dictgxbrutasen"),
    # 3. Mercado y balanza comercial de combustibles
    _config("ventas", "ea/venta/combustibleliquido/mensual"),
    _config("importacion", "ea/importacion"),
    _config("exportacion", "ea/exportacion"),
    _config("pib_regional", "ea/pibregionalega"),
    # Dimensiones geográficas
    _config("regiones", "region"),
    _config("comunas", *[f"comuna/{r}" for r in CODIGOS_REGIONES]),
]
