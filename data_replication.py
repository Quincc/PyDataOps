from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

def replicate_data():
    # Подключение к PostgreSQL
    postgres = PostgresHook(postgres_conn_id="postgres_conn")
    source_conn = postgres.get_conn()
    source_cursor = source_conn.cursor()

    # Подключение к MySQL
    mysql = MySqlHook(mysql_conn_id="mysql_conn")
    target_conn = mysql.get_conn()
    target_cursor = target_conn.cursor()

    # Чтение данных из PostgreSQL
    source_cursor.execute("SELECT * FROM users")  # Замените на вашу таблицу
    rows = source_cursor.fetchall()

    # Очистка таблицы в MySQL перед записью (опционально)
    target_cursor.execute("TRUNCATE TABLE users")  # Замените на вашу таблицу

    # Загрузка данных в MySQL
    for row in rows:
        target_cursor.execute(
            "INSERT INTO users (id, first_name, last_name, email, registration_date) VALUES (%s, %s, %s, %s, %s)",
            row
        )
    target_conn.commit()

    source_cursor.close()
    target_cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG('data_replication', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    replicate_task = PythonOperator(
        task_id='replicate_data',
        python_callable=replicate_data
    )
