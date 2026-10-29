from endpoints import API_CONFIGS
#Para tener control de TIEMPO 
from datetime import datetime, timedelta
#Para las funciones DAG de Airflow
from airflow.decorators import dag, task

import os
import requests
import pandas as pd
import json

DEFAULT_ARGS = {                            #Parametros para no repetir codigo 
    'owner': 'airflow',                     #usuario
    'depends_on_past': False,               #depende del exito de otra tarea
    'email_on_failure': False,              #notificar por correo si falla
    'retries': 1,                           #numero de reintentos
    'retry_delay': timedelta(minutes=5),    #espera entre reintentos
}

@dag(
    dag_id='etl_CMF',
    default_args=DEFAULT_ARGS,
    description='ETL pipeline para CMF',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etls', 'chile', 'apis'],
)



def etl_pipeline():
    @task()
    def extract_data(config: dict) -> dict:             #dict es para crear librerias o JSON por lo que entendi
        api_key = os.getenv("API_KEY")      #obtenemos la API almacenada en el .env

        url = f"{config['url_template']}?apikey={api_key}&formato=json"
        #url = f"https://api.cmfchile.cl/api-sbifv3/recursos_api/balances/2009/12/cuentas/1000000?apikey=SBIF9990SBIF44b7SBIF7f4c5a537d02358e1099&formato=xml"

        headers = {
            "Accept": "application/json"            #en que formato quieres la respuesta
        }

        response = requests.get(url, headers=headers, timeout=30)   #empieza la extraccion de datos
        if response.status_code == 404:
            print(f"Aviso: El recurso no existe para [{config['id_fuente']}]. Se omite.")
            return {"id_fuente": config['id_fuente'], "raw_data": None}
        
        response.raise_for_status()                                 #verifica si la solicitud fue exitosa
        raw_data = response.json()  #se almacenan los datos en formato JSON de respuesta

        filename = f"/opt/airflow/dags/raw_{config['id_fuente']}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)
        
        print(f"Datos extraídos exitosamente: [{config['id_fuente']}].")
        return {
            "id_fuente": config['id_fuente'],
            "tipo": config['tipo'],
            "raw_data": raw_data
        }
    extracted_data_list = extract_data.expand(config=API_CONFIGS)


dag_instance = etl_pipeline()