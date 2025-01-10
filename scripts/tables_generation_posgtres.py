import psycopg2
import random
from faker import Faker

# Установите соединение с PostgreSQL
def create_connection():
    return psycopg2.connect(
        dbname="Hse",
        user="postgres",
        password="pass",
        host="localhost",
        port="5432"
    )
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Users (
            user_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(15),
            registration_date DATE,
            loyalty_status VARCHAR(10)
        );

        CREATE TABLE IF NOT EXISTS Products (
            product_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            description TEXT,
            category_id INT,
            price DECIMAL(10, 2),
            stock_quantity INT,
            creation_date DATE
        );

        CREATE TABLE IF NOT EXISTS Orders (
            order_id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            order_date TIMESTAMP,
            total_amount DECIMAL(10, 2),
            status VARCHAR(20),
            delivery_date DATE,
            FOREIGN KEY (user_id) REFERENCES Users(user_id)
        );

        CREATE TABLE IF NOT EXISTS OrderDetails (
            order_detail_id BIGSERIAL PRIMARY KEY,
            order_id BIGINT NOT NULL,
            product_id BIGINT NOT NULL,
            quantity INT NOT NULL,
            price_per_unit NUMERIC(10, 2) NOT NULL,
            total_price NUMERIC(10, 2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES Orders(order_id),
            FOREIGN KEY (product_id) REFERENCES Products(product_id)
        );

        CREATE TABLE IF NOT EXISTS ProductCategories (
            category_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            parent_category_id INT
        );
        """)
        conn.commit()

# Основной скрипт
if __name__ == "__main__":
    conn = create_connection()
    create_tables(conn)
    conn.close()