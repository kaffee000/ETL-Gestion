FROM apache/airflow:3.3.2
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary openpyxl matplotlib
