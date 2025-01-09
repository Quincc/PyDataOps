def replicate_table(table_name):
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
        target_cursor.execute(f"TRUNCATE TABLE {table_name}")

        # Вставка данных в MySQL
        for row in rows:
            placeholders = ', '.join(['%s'] * len(row))
            target_cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)

        target_conn.commit()
        print(f"Таблица {table_name} успешно реплицирована.")

    except Exception as e:
        print(f"Ошибка при репликации таблицы {table_name}: {e}")

    finally:
        source_cursor.close()
        target_cursor.close()

# DAG для репликации данных
with DAG(
    'replicate_all_tables',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    tables = ['users', 'products', 'orders', 'order_details', 'product_categories']

    for table in tables:
        PythonOperator(
            task_id=f'replicate_{table}',
            python_callable=replicate_table,
            op_args=[table]
        )
