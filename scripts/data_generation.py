import psycopg2
import random
from faker import Faker

# Установите соединение с PostgreSQL
def create_connection():
    return psycopg2.connect(
        dbname="Hse",
        user="postgres",
        password="1331",
        host="localhost",
        port="5432"
    )

# Создание таблиц
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Users (
            user_id SERIAL PRIMARY KEY,
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
            order_id SERIAL PRIMARY KEY,
            user_id INT REFERENCES Users(user_id),
            order_date TIMESTAMP,
            total_amount DECIMAL(10, 2),
            status VARCHAR(20),
            delivery_date DATE
        );

        CREATE TABLE IF NOT EXISTS OrderDetails (
            order_detail_id SERIAL PRIMARY KEY,
            order_id INT REFERENCES Orders(order_id),
            product_id INT REFERENCES Products(product_id),
            quantity INT,
            price_per_unit DECIMAL(10, 2),
            total_price DECIMAL(10, 2)
        );

        CREATE TABLE IF NOT EXISTS ProductCategories (
            category_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            parent_category_id INT
        );
        """)
        conn.commit()

# Генерация данных
def generate_data(conn):
    fake = Faker()
    with conn.cursor() as cur:
        # Генерация пользователей
        for _ in range(100):
            cur.execute(
                "INSERT INTO Users (first_name, last_name, email, phone, registration_date, loyalty_status) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    fake.first_name(),
                    fake.last_name(),
                    fake.email(),
                    fake.phone_number()[:15],  # Ограничение длины до 15 символов
                    fake.date_this_decade(),
                    random.choice(["Gold", "Silver", "Bronze"])
                )
            )

        # Генерация категорий товаров
        for _ in range(10):
            cur.execute(
                "INSERT INTO ProductCategories (name, parent_category_id) VALUES (%s, %s)",
                (fake.word(), random.choice([None, random.randint(1, 5)]))
            )

        # Генерация товаров
        for _ in range(100):
            cur.execute(
                "INSERT INTO Products (name, description, category_id, price, stock_quantity, creation_date) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    fake.word(),
                    fake.text(),
                    random.randint(1, 10),
                    round(random.uniform(10, 100), 2),
                    random.randint(1, 500),
                    fake.date_this_decade()
                )
            )

        # Генерация заказов
        for _ in range(100):
            user_id = random.randint(1, 100)
            total_amount = round(random.uniform(20, 500), 2)
            cur.execute(
                "INSERT INTO Orders (user_id, order_date, total_amount, status, delivery_date) VALUES (%s, %s, %s, %s, %s)",
                (
                    user_id,
                    fake.date_time_this_year(),
                    total_amount,
                    random.choice(["Pending", "Completed", "Cancelled"]),
                    fake.date_this_year()
                )
            )

        conn.commit()

# Основной скрипт
if __name__ == "__main__":
    conn = create_connection()
    create_tables(conn)
    generate_data(conn)
    conn.close()
