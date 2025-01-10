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

# Создание таблиц
# def create_tables(conn):
#     with conn.cursor() as cur:
#         cur.execute("""
#         CREATE TABLE IF NOT EXISTS Users (
#             user_id INT PRIMARY KEY,
#             first_name VARCHAR(50),
#             last_name VARCHAR(50),
#             email VARCHAR(100),
#             phone VARCHAR(15),
#             registration_date DATE,
#             loyalty_status VARCHAR(10)
#         );
#
#         CREATE TABLE IF NOT EXISTS Products (
#             product_id SERIAL PRIMARY KEY,
#             name VARCHAR(100),
#             description TEXT,
#             category_id INT,
#             price DECIMAL(10, 2),
#             stock_quantity INT,
#             creation_date DATE
#         );
#
#         CREATE TABLE IF NOT EXISTS Orders (
#             order_id BIGSERIAL PRIMARY KEY,
#             user_id BIGINT NOT NULL,
#             order_date TIMESTAMP,
#             total_amount DECIMAL(10, 2),
#             status VARCHAR(20),
#             delivery_date DATE,
#             FOREIGN KEY (user_id) REFERENCES Users(user_id)
#         );
#
#         CREATE TABLE IF NOT EXISTS OrderDetails (
#             order_detail_id BIGSERIAL PRIMARY KEY,
#             order_id BIGINT NOT NULL,
#             product_id BIGINT NOT NULL,
#             quantity INT NOT NULL,
#             price_per_unit NUMERIC(10, 2) NOT NULL,
#             total_price NUMERIC(10, 2) NOT NULL,
#             FOREIGN KEY (order_id) REFERENCES Orders(order_id),
#             FOREIGN KEY (product_id) REFERENCES Products(product_id)
#         );
#
#         CREATE TABLE IF NOT EXISTS ProductCategories (
#             category_id SERIAL PRIMARY KEY,
#             name VARCHAR(100),
#             parent_category_id INT
#         );
#         """)
#         conn.commit()

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
        # Генерация деталей заказов
        cur.execute("SELECT order_id FROM Orders")
        order_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT product_id FROM Products")
        product_ids = [row[0] for row in cur.fetchall()]

        for order_id in order_ids:
            for _ in range(random.randint(1, 5)):
                product_id = random.choice(product_ids)
                quantity = random.randint(1, 10)
                price_per_unit = round(random.uniform(10, 100), 2)
                total_price = round(quantity * price_per_unit, 2)
                cur.execute(
                    "INSERT INTO OrderDetails (order_id, product_id, quantity, price_per_unit, total_price) VALUES (%s, %s, %s, %s, %s)",
                    (order_id, product_id, quantity, price_per_unit, total_price)
                )

        conn.commit()

# Основной скрипт
if __name__ == "__main__":
    conn = create_connection()
    # create_tables(conn)
    generate_data(conn)
    conn.close()
