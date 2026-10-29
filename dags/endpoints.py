# Archivo: dags/endpoints.py
API_CONFIGS = [
    {
        "id_fuente": "balance_cuenta_1000000_2009_12",
        "url_template": "https://api.cmfchile.cl/api-sbifv3/recursos_api/balances/2009/12/cuentas/1000000",
        "tipo": "balance"
    }
]

"""
# Parámetros generales para los endpoints mensuales (2000 a 2024)
anios_mensuales = range(2000, 2025)
meses = [f"{m:02d}" for m in range(1, 13)]
institucion_global = "999"

# 1, 2 y 4. Endpoints mensuales habituales (Adecuación, Balances y Fichas)
for anio in anios_mensuales:
    for mes in meses:
        
        # Adecuación de capital (IRS)
        API_CONFIGS.append({
            "id_fuente": f"adecuacion_irs_{anio}_{mes}",
            "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/adecuacion/anhos/{anio}/meses/{mes}/instituciones/{institucion_global}/indicadores/irs",
            "tipo": "adecuacion"
        })
        # Balance mensual de bancos
        API_CONFIGS.append({
            "id_fuente": f"balances_bancos_{anio}_{mes}",
            "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/balances/{anio}/{mes}/instituciones",
            "tipo": "balance"
        })
        # Fichas bancarias (Perfil)
        API_CONFIGS.append({
            "id_fuente": f"perfil_bancario_{anio}_{mes}",
            "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/perfil/instituciones/{institucion_global}/{anio}/{mes}",
            "tipo": "perfil"
        })

# 3. Estado de resultado de bancos (Exclusivo desde el 2008 en adelante y de carácter anual)
anios_resultados = range(2008, 2025)

for anio in anios_resultados:
    API_CONFIGS.append({
        "id_fuente": f"resultados_bancos_{anio}",
        "url_template": f"https://api.cmfchile.cl/api-sbifv3/recursos_api/resultados/{anio}/instituciones",
        "tipo": "resultados"
    })
"""