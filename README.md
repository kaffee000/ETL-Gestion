# ETL Pipeline CMF Chile - Apache Airflow & PostgreSQL

Este repositorio contiene la arquitectura de un pipeline ETL desarrollado en **Apache Airflow** para extraer, transformar y consolidar información financiera proveniente de la API de la CMF (Comisión para el Mercado Financiero de Chile) correspondiente a 5 instituciones bancarias durante el período 2020–2024.

---

## 📋 Prerrequisitos

Asegúrate de contar con las siguientes herramientas instaladas en tu entorno local:

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (con integración WSL 2 habilitada)
* [PostgreSQL](https://www.postgresql.org/)
* [DBeaver](https://dbeaver.io/) (o cualquier cliente de base de datos de tu preferencia)

---

## 🚀 Guía de Instalación y Despliegue

Sigue estos pasos en **PowerShell** para desplegar la infraestructura en tu máquina local.

### Paso 1: Configurar WSL (Windows Subsystem for Linux)
Abre PowerShell como **Administrador** y ejecuta:

```powershell
wsl --install
```

> **Nota:** Reinicia el equipo. Tras reiniciar, verifica la versión instalada:

```powershell
wsl --status
```
*Debe indicar que la versión predeterminada es la 2. Si marca versión 1, ejecuta:*

```powershell
wsl --set-default-version 2
```

---

### Paso 2: Verificar la Instalación de Docker

```powershell
docker --version
docker compose version
```

---

### Paso 3: Crear la Estructura de Directorios

En PowerShell normal, crea la carpeta del proyecto y descarga el archivo base de Docker Compose:

```powershell
mkdir C:\airflow-docker
cd C:\airflow-docker
curl.exe -LfO "https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml"
mkdir dags, logs, plugins, config
dir
```

---

### Paso 4: Configurar `.env` y Crear `Dockerfile`

1. Define el UID de usuario para Airflow:

```powershell
[System.IO.File]::WriteAllText("C:\airflow-docker\.env", "AIRFLOW_UID=50000`n")
Get-Content .env
```

2. Genera el `Dockerfile` agregando las dependencias de Python necesarias (`pandas`, `sqlalchemy`, `psycopg2-binary`, etc.):

```powershell
$c = "FROM apache/airflow:3.3.2`n"
$c += "RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary openpyxl matplotlib`n"
[System.IO.File]::WriteAllText("C:\airflow-docker\Dockerfile", $c)
Get-Content Dockerfile
```

---

### Paso 5: Ajustar Parámetros en `docker-compose.yaml`

> ⚠️ **IMPORTANTE:** Reemplaza los valores de `$db`, `$usr` y `$pw` con las credenciales reales de tu instancia local de PostgreSQL.

```powershell
$f = "C:\airflow-docker\docker-compose.yaml"
$db = "NOMBRE_DE_TU_BASE"
$usr = "postgres"
$pw = "TU_CONTRASENA"
$pgport = "5432"
$web = "8080"
$t = [System.IO.File]::ReadAllText($f)
$p1 = '(?m)^(\s*)image: \$\{AIRFLOW_IMAGE_NAME:-[^}]*\}'
$t = $t -replace $p1, '$1# image: ${AIRFLOW_IMAGE_NAME}'
$t = $t -replace '(?m)^(\s*)# build: \.', '$1build: .'
$nuevo = @(
    "`$1AIRFLOW__CORE__LOAD_EXAMPLES: 'false'",
    "`$1PGHOST: host.docker.internal",
    "`$1PGPORT: '$pgport'",
    "`$1PGDATABASE: $db",
    "`$1PGUSER: $usr",
    "`$1PGPASSWORD: '$pw'"
) -join "`n"
$p3 = "(?m)^(\s*)AIRFLOW__CORE__LOAD_EXAMPLES: 'true'"
$t = $t -replace $p3, $nuevo
$t = $t.Replace('8080:8080', "${web}:8080")
[System.IO.File]::WriteAllText($f, $t)
Select-String -Path $f -Pattern "image:|build:|LOAD_EXAMPLES|PG[A-Z]+:|:8080"
```

---

### Paso 6: Construir la Imagen y Desplegar Contenedores

Ubicado en la ruta `C:\airflow-docker`:

```powershell
# 1. Construir la imagen personalizada
docker compose build

# 2. Inicializar la base de datos de Airflow
docker compose up airflow-init

# 3. Encender los servicios en segundo plano
docker compose up -d

# 4. Verificar el estado de los contenedores
docker compose ps
```

---

### Paso 7: Configuración de Variables y DAGs

1. Asegúrate de configurar la clave de la API (`API_KEY`) y la URI de la base de datos (`DATABASE_URL`) en el archivo `.env`.
2. Copia todos los scripts en Python (`endpoints.py`, `etl_cmf.py`, etc.) dentro de la carpeta local `./dags`.

---

### Paso 8: Acceso a la Interfaz Web de Airflow

Una vez que los contenedores reporten un estado *healthy*:

* **URL:** [http://localhost:8080](http://localhost:8080)
* **Usuario:** `airflow`
* **Contraseña:** `airflow`

---

## 🛠️ Estructura del Pipeline ETL

1. **`extract_data`**: Peticiones concurrentes a los distintos endpoints de la CMF (adecuación, balances, resultados, accionistas) con control de excepciones para `Timeout`, errores `404` y `500`.
2. **`transform_data`**: Normalización, remoción de nulos y duplicados mediante `pandas`, consolidando la información de manera segura utilizando bloqueo de archivos (`fcntl`).
3. **`load_to_postgres`**: Carga masiva mediante `SQLAlchemy` a PostgreSQL.
