# Archivo: dags/endpoints.py
API_CONFIGS = []

# Parámetros generales
ANIOS = range(2020, 2023)  # 2020 a 2022
MESES = [f"{m:02d}" for m in range(1, 13)]  # "01" a "12"
CODIGOS_INSTITUCIONES = ["001", "016", "012", "037", "028"]  # Primeros 5 bancos

# 1. Adecuación de capital (Bucle por año, mes e institución)
for ano in ANIOS:
    for mes in MESES:
        for institucion in CODIGOS_INSTITUCIONES:
            API_CONFIGS.append({
                "id_fuente": f"adecuacion_{institucion}_{ano}_{mes}",
                "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/adecuacion/anhos/{ano}/meses/{mes}/instituciones/{institucion}/componentes",
                "tipo": "adecuacion"
            })

# 2. Balance del banco (Bucle por año e institución)
for ano in ANIOS:
    for institucion in CODIGOS_INSTITUCIONES:
        API_CONFIGS.append({
            "id_fuente": f"balances_{institucion}_{ano}",
            "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/balances/{ano}/instituciones/{institucion}",
            "tipo": "balances"
        })

# 3. Estado de resultados (Bucle por año e institución)
for ano in ANIOS:
    for institucion in CODIGOS_INSTITUCIONES:
        API_CONFIGS.append({
            "id_fuente": f"resultados_{institucion}_{ano}",
            "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/resultados/{ano}/instituciones/{institucion}",
            "tipo": "resultados"
        })

# 4. Fichas bancarias accionistas (Bucle por institución, año y mes)
for institucion in CODIGOS_INSTITUCIONES:
    for ano in ANIOS:
        for mes in MESES:
            API_CONFIGS.append({
                "id_fuente": f"accionistas_{institucion}_{ano}_{mes}",
                "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/accionistas/instituciones/{institucion}/anhos/{ano}/meses/{mes}/ficha",
                "tipo": "accionistas"
            })
