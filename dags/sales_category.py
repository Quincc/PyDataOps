from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

def create_sales_by_category_table():
    mysql = MySqlHook(mysql_conn_id="mysql_conn")
    mysql_conn = mysql.get_conn()
    cursor = mysql_conn.cursor()

    # Создание таблицу sales_category
    query_create_table = """
        CREATE TABLE IF NOT EXISTS SalesByCategory (
            category_id INT PRIMARY KEY,
            category_name VARCHAR(100),
            total_sales NUMERIC(15, 2),
            total_quantity INT,
            num_orders INT
        );
        """
    cursor.execute(query_create_table)
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

def populate_sales_by_category_table():
    mysql = MySqlHook(mysql_conn_id="mysql_conn")
    mysql_conn = mysql.get_conn()
    cursor = mysql_conn.cursor()

    # Заполнение витрины
    query_insert_data = """
        INSERT INTO SalesByCategory (category_id, category_name, total_sales, total_quantity, num_orders)
        SELECT
            pc.category_id,
            pc.name AS category_name,
            SUM(od.total_price) AS total_sales,
            SUM(od.quantity) AS total_quantity,
            COUNT(DISTINCT o.order_id) AS num_orders
        FROM
            OrderDetails od
        JOIN
            Products p ON od.product_id = p.product_id
        JOIN
            ProductCategories pc ON p.category_id = pc.category_id
        JOIN
            Orders o ON od.order_id = o.order_id
        GROUP BY
            pc.category_id, pc.name;
        """
    cursor.execute(query_insert_data)
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()

# Определение DAG
with DAG(
        dag_id='sales_by_category_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@daily',
        catchup=False,
) as dag:
    # Задача 1: Создание таблицы sales_by_category
    create_table_task = PythonOperator(
        task_id='create_sales_by_category_table',
        python_callable=create_sales_by_category_table,
    )

    # Задача 2: Заполнение таблицы sales_by_category
    populate_table_task = PythonOperator(
        task_id='populate_sales_by_category_table',
        python_callable=populate_sales_by_category_table,
    )

    # Последовательность выполнения задач
    create_table_task >> populate_table_task