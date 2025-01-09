from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pymysql

def create_user_activity_table():
    mysql = MySqlHook(mysql_conn_id="mysql_conn")
    mysql_conn = mysql.get_conn()
    cursor= mysql_conn.cursor()

    # Создаем таблицу user_activity
    query_create_table = """
        CREATE TABLE IF NOT EXISTS user_activity (
            user_id INT,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            total_orders INT,
            total_spent DECIMAL(10, 2),
            last_order_date TIMESTAMP,
        );
    """
    cursor.execute(query_create_table)
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

# Функция для заполнения таблицы
def populate_user_activity_table():
    mysql = MySqlHook(mysql_conn_id="mysql_conn")
    mysql_conn = mysql.get_conn()
    cursor= mysql_conn.cursor()

    # Вставляем данные в таблицу user_activity
    query_insert_data = """
        INSERT INTO user_activity (user_id, first_name, last_name, total_orders, total_spent, last_order_date)
        SELECT
            u.user_id,
            u.first_name,
            u.last_name,
            COUNT(o.order_id) AS total_orders,
            SUM(o.total_amount) AS total_spent,
            MAX(CASE 
                    WHEN o.order_date = '0000-00-00 00:00:00' OR o.order_date IS NULL THEN NULL
                    ELSE o.order_date
                END) AS last_order_date
        FROM
            users u
        LEFT JOIN
            orders o ON u.user_id = o.user_id
        GROUP BY
            u.user_id, u.first_name, u.last_name;
    """
    cursor.execute(query_insert_data)
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

# Определение DAG
with DAG(
        dag_id='user_activity_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@daily',
        catchup=False,
) as dag:
    # Задача 1: Создание таблицы user_activity
    create_table_task = PythonOperator(
        task_id='create_user_activity_table',
        python_callable=create_user_activity_table,
    )

    # Задача 2: Заполнение таблицы user_activity
    populate_table_task = PythonOperator(
        task_id='populate_user_activity_table',
        python_callable=populate_user_activity_table,
    )

    # Последовательность выполнения задач
    create_table_task >> populate_table_task