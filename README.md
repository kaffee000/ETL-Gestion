# ETL de datos abiertos de Chile: CMF y CNE

Procesos ETL en Python, orquestados con Apache Airflow, que consumen las APIs de la CMF y la CNE,
cargan los datos en PostgreSQL y calculan 6 indicadores de gestión en SQL.

## Requisitos

- Docker y Docker Compose.
- Una base PostgreSQL accesible desde los contenedores (`DATABASE_URL`).
- Una clave de la API CMF y una cuenta registrada en https://api.cne.cl/register.

## Puesta en marcha

1. Copia `.env.example` como `.env` y completa los valores. Si un valor lleva `$`, ponlo entre comillas simples.
2. Levanta Airflow:
   ```bash
   docker compose up -d --build
   ```
3. Entra a http://localhost:8080 con el usuario definido en `docker-compose.yaml`.
4. Activa y ejecuta los DAGs en este orden: `etl_cmf_indicadores`, `indicadores_gestion`, `etl_cne`.

El esquema se crea solo con la tarea `aplicar_ddl`. También se puede crear a mano con `dags/sql/ddl.sql`.

Si cambias `.env`, recrea los contenedores con `docker compose up -d`: no leen el archivo mientras corren.

## DAGs

| DAG | Función | Programación |
|---|---|---|
| `etl_cne` | API CNE hacia `raw`, `staging` y `dw`, con auditoría | Diario, 06:00 de Chile |
| `etl_cmf_indicadores` | API CMF hacia tablas `cmf_ind_*` del esquema `public`, con auditoría | Mensual, día 10 a las 06:00 de Chile |
| `indicadores_gestion` | KPIs en SQL sobre `dw`, hacia `mart` | Al terminar la carga de `etl_cne` |

## Demostración de falla controlada

En Airflow, usa **Trigger** en `etl_cne` con esta configuración:

```json
{"fuente_falla": "ventas"}
```

En `etl_cmf_indicadores` funciona igual, usando el id de una fuente:

```json
{"fuente_falla": "balances_001_2020"}
```

`extraer_ventas` usa una URL incorrecta y falla tras 3 intentos con espera creciente. Las demás fuentes
continúan, `cargar_dw` se ejecuta, se dispara `indicadores_gestion` y el run queda marcado como fallido.
Para recuperarse, basta con ejecutar el DAG sin configuración. Luego se puede revisar el registro:

```sql
SELECT task_id, intento, estado, mensaje
FROM audit.etl_audit
WHERE estado <> 'success'
ORDER BY inicio DESC;
```

## Recarga de un rango de años

Usa **Trigger** en `etl_cne` con el rango que quieres reemplazar en `dw`:

```json
{"anio_desde": 2024, "anio_hasta": 2025}
```

Los precios de estaciones no se pueden recargar hacia atrás, porque la API solo entrega el valor vigente.

## Pruebas

Se ejecutan dentro del worker y usan datos simulados:

```bash
docker cp tests/test_cne.py airflow-docker-airflow-worker-1:/tmp/test_cne.py
```

```bash
docker exec -e PYTHONPATH=/opt/airflow/dags airflow-docker-airflow-worker-1 python /tmp/test_cne.py
```

## Estructura

```text
dags/
  etl_cne.py               DAG ETL de la CNE
  etl_cmf_indicadores.py   DAG ETL de la CMF
  indicadores_gestion.py   DAG de indicadores
  cne_cliente.py           Login, renovación de token, paginación y errores de la API CNE
  transform_cne.py         Limpieza y validaciones de la CNE
  indicadores.py           Lectura de cuentas contables de la CMF
  indicadores_cne.py       Correlaciones entre CNE y CMF
  etl_comun.py             Conexión, ejecución de SQL y auditoría
  endpoints_cne.py         Endpoints de la CNE
  endpoints_indicadores.py Endpoints de la CMF
  sql/ddl.sql              DDL de raw, staging, dw, mart y audit
  sql/dw_cne.sql           Carga idempotente de staging a dw
  sql/mart_kpis.sql        Cálculo de los KPIs
tests/test_cne.py          Pruebas unitarias
```

## Seguridad

Las credenciales solo van en `.env`, que está excluido por `.gitignore` y no debe entregarse ni subirse.
