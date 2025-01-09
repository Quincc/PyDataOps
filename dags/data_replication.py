from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

def clean_timestamp(value):
    if value in ['0000-00-00 00:00:00', None]:
        return None
    return value

def replicate_data(table_name):
    try:
        # Подключение к PostgreSQL
        postgres = PostgresHook(postgres_conn_id="postgres_conn")
        source_conn = postgres.get_conn()
        source_cursor = source_conn.cursor()

        # Подключение к MySQL
        mysql = MySqlHook(mysql_conn_id="mysql_conn")
        target_conn = mysql.get_conn()
        target_cursor = target_conn.cursor()

        # Извлечение данных из PostgreSQL
        source_cursor.execute(f"SELECT * FROM {table_name}")
        rows = source_cursor.fetchall()

        # Очистка таблицы в MySQL перед загрузкой
        target_cursor.execute(f"DELETE FROM {table_name}")

        # Отключение проверки внешних ключей временно
        target_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        # Вставка данных в MySQL
        for row in rows:
            clean_row = [clean_timestamp(value) if isinstance(value, str) else value for value in row]
            placeholders = ', '.join(['%s'] * len(clean_row))
            target_cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)

        # Включение проверки внешних ключей обратно
        target_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        target_conn.commit()
        print(f"Таблица {table_name} успешно реплицирована.")

    except Exception as e:
        print(f"Ошибка при репликации таблицы {table_name}: {e}")

    finally:
        source_cursor.close()
        target_cursor.close()

# DAG для репликации данных
with DAG(
    'data_replication',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    tables = ['users', 'products', 'orders', 'orderDetails', 'productCategories']

    for table in tables:
        PythonOperator(
            task_id=f'replicate_{table}',
            python_callable=replicate_data,
            op_args=[table]
        )

# def replicate_data():
#     # Подключение к PostgreSQL
#     postgres = PostgresHook(postgres_conn_id="postgres_conn")
#     source_conn = postgres.get_conn()
#     source_cursor = source_conn.cursor()
#
#     # Подключение к MySQL
#     mysql = MySqlHook(mysql_conn_id="mysql_conn")
#     target_conn = mysql.get_conn()
#     target_cursor = target_conn.cursor()
#
#     # Чтение данных из PostgreSQL
#     source_cursor.execute("SELECT * FROM users")  # Замените на вашу таблицу
#     rows = source_cursor.fetchall()
#
#     # Очистка таблицы в MySQL перед записью (опционально)
#     target_cursor.execute("DELETE FROM users")  # Замените на вашу таблицу
#
#     # Загрузка данных в MySQL
#     for row in rows:
#         target_cursor.execute(
#             "INSERT INTO users (user_id, first_name, last_name, email, phone, registration_date, loyalty_status ) VALUES (%s, %s, %s, %s, %s, %s, %s)",
#             row
#         )
#     target_conn.commit()
#
#     source_cursor.close()
#     target_cursor.close()
#
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 1, 1),
#     'retries': 1,
# }
#
# with DAG('data_replication', default_args=default_args, schedule='@daily', catchup=False) as dag:
#     replicate_task = PythonOperator(
#         task_id='replicate_data',
#         python_callable=replicate_data
#     )
