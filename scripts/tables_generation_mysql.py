import mysql.connector

# Подключение к MySQL
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="pass",
        database="hse"
    )

# Функция создания таблиц
def create_tables(conn):
    with conn.cursor() as cursor:
        # Создание таблиц
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Users (
            user_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(15),
            registration_date DATE,
            loyalty_status VARCHAR(10)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products (
            product_id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100),
            description TEXT,
            category_id INT,
            price DECIMAL(10, 2),
            stock_quantity INT,
            creation_date DATE
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Orders (
            order_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            user_id INT NOT NULL,
            order_date TIMESTAMP,
            total_amount DECIMAL(10, 2),
            status VARCHAR(20),
            delivery_date DATE,
            FOREIGN KEY (user_id) REFERENCES Users(user_id)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS OrderDetails (
            order_detail_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            order_id BIGINT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            price_per_unit DECIMAL(10, 2) NOT NULL,
            total_price DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES Orders(order_id),
            FOREIGN KEY (product_id) REFERENCES Products(product_id)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ProductCategories (
            category_id INT PRIMARY KEY AUTO_INCREMENT,
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
