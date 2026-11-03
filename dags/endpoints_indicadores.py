# Archivo: dags/endpoints_indicadores.py
# Lista de endpoints de la API CMF (api-sbifv3) que el DAG consulta.
# Cada config lleva la institución, el año y el mes para que los indicadores
# puedan agrupar por banco y período aunque la API no los devuelva en la respuesta.

API_BASE = "https://api.cmfchile.cl/api-sbifv3/recursos_api"

ANIOS = range(2020, 2026)  # 2020 a 2025
MESES = [f"{m:02d}" for m in range(1, 13)]
CODIGOS_INSTITUCIONES = ["001", "016", "012", "037", "028"]  # 5 bancos

API_CONFIGS = []

# 1. Balance del banco (año e institución). Fuente de activos, pasivos y patrimonio.
for anio in ANIOS:
    for inst in CODIGOS_INSTITUCIONES:
        API_CONFIGS.append({
            "id_fuente": f"balances_{inst}_{anio}",
            "url_template": f"{API_BASE}/balances/{anio}/instituciones/{inst}",
            "tipo": "balances",
            "institucion": inst,
            "anio": anio,
            "mes": None,
        })

# 2. Estado de resultados (año e institución). Fuente de la utilidad del ejercicio.
for anio in ANIOS:
    for inst in CODIGOS_INSTITUCIONES:
        API_CONFIGS.append({
            "id_fuente": f"resultados_{inst}_{anio}",
            "url_template": f"{API_BASE}/resultados/{anio}/instituciones/{inst}",
            "tipo": "resultados",
            "institucion": inst,
            "anio": anio,
            "mes": None,
        })

# 3. Adecuación de capital (solo diciembre de cada año, para no saturar la API).
for anio in ANIOS:
    for inst in CODIGOS_INSTITUCIONES:
        API_CONFIGS.append({
            "id_fuente": f"adecuacion_{inst}_{anio}_12",
            "url_template": f"{API_BASE}/adecuacion/anhos/{anio}/meses/12/instituciones/{inst}/componentes",
            "tipo": "adecuacion",
            "institucion": inst,
            "anio": anio,
            "mes": "12",
        })
