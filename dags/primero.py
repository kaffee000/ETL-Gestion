from datetime import datetime, timedelta
# Para las funciones DAG de Airflow
from airflow.decorators import dag, task
from endpoints import API_CONFIGS
import os
import requests
import pandas as pd
import json
import fcntl

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="etl_CMF",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline para CMF",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etls", "chile", "apis"],
)
def etl_pipeline():

  @task()
  def extract_data(config: dict) -> dict:
    """Extrae los datos de la API manejando timeouts y errores de servidor de forma segura."""
    api_key = os.getenv("API_KEY")

    url = f"{config['url_template']}?apikey={api_key}&formato=json"
    headers = {"Accept": "application/json"}

    try:
      # Se establece un timeout de 30 segundos
      response = requests.get(url, headers=headers, timeout=30)
      
      # Si el servidor responde con errores conocidos (404, 500, etc.)
      if response.status_code in [404, 500, 502, 503, 504]:
        print(f"Aviso: El recurso [{config['id_fuente']}] devolvió el código HTTP {response.status_code}. Se omite.")
        return {"id_fuente": config["id_fuente"], "tipo": config["tipo"], "raw_data": None}

      response.raise_for_status()
      raw_data = response.json()

    except requests.exceptions.Timeout:
      print(f"Aviso: Timeout (se superó el tiempo límite) al intentar conectar con [{config['id_fuente']}]. Se omite.")
      return {"id_fuente": config["id_fuente"], "tipo": config["tipo"], "raw_data": None}
    except requests.exceptions.RequestException as e:
      print(f"Aviso: Error de conexión en [{config['id_fuente']}]: {e}. Se omite.")
      return {"id_fuente": config["id_fuente"], "tipo": config["tipo"], "raw_data": None}

    return {
        "id_fuente": config["id_fuente"],
        "tipo": config["tipo"],
        "raw_data": raw_data,
    }

  @task()
  def transform_data(extract_result: dict) -> dict:
    """Transforma, limpia y guarda los datos directamente en el JSON único de su categoría."""
    id_fuente = extract_result.get("id_fuente")
    tipo = extract_result.get("tipo")
    raw_data = extract_result.get("raw_data")

    if not raw_data:
      return {"id_fuente": id_fuente, "tipo": tipo, "status": "skipped"}

    # Normalización a DataFrame
    if isinstance(raw_data, dict):
      list_keys = [k for k, v in raw_data.items() if isinstance(v, list)]
      if list_keys:
        df = pd.json_normalize(raw_data[list_keys[0]])
      else:
        df = pd.json_normalize([raw_data])
    elif isinstance(raw_data, list):
      df = pd.json_normalize(raw_data)
    else:
      df = pd.DataFrame()

    if df.empty:
      return {"id_fuente": id_fuente, "tipo": tipo, "status": "empty"}

    # Limpieza de duplicados y valores nulos
    df = df.drop_duplicates()
    df = df.fillna(value="")

    records = df.to_dict(orient="records")

    # Directorio y archivo único por categoría dentro de la misma ruta
    output_dir = "/opt/airflow/dags/datos_limpios"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"datos_limpios_{tipo}.json")

    # Escritura segura con bloqueo (fcntl) para evitar conflictos entre tareas paralelas de Airflow
    with open(filename, "a+", encoding="utf-8") as f:
      fcntl.flock(f, fcntl.LOCK_EX)
      try:
        f.seek(0)
        content = f.read()
        existing_data = json.loads(content) if content.strip() else []
        
        # Agregar los nuevos registros limpios
        existing_data.extend(records)
        
        f.seek(0)
        f.truncate()
        json.dump(existing_data, f, ensure_ascii=False, indent=4)
      finally:
        fcntl.flock(f, fcntl.LOCK_UN)

    print(f"Transformación y guardado exitoso para [{id_fuente}] en [{filename}]")
    return {
        "id_fuente": id_fuente,
        "tipo": tipo,
        "status": "success"
    }

  # Encadenamiento del flujo optimizado (Extract -> Transform)
  extracted_data_list = extract_data.expand(config=API_CONFIGS)
  transform_data.expand(extract_result=extracted_data_list)


dag_instance = etl_pipeline()