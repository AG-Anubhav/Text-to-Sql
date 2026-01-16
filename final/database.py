import sqlite3
import random
from datetime import datetime, timedelta
from faker import Faker # You might need to install this: pip install faker

# Initialize Faker for realistic names/emails
fake = Faker()

def init_db():
    conn = sqlite3.connect("sales_data.db")
    cursor = conn.cursor()

    # --- DROP TABLES TO RESTART CLEAN ---
    cursor.execute("DROP TABLE IF EXISTS sales")
    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.execute("DROP TABLE IF EXISTS regions")
    cursor.execute("DROP TABLE IF EXISTS customers")

    # --- 1. CREATE TABLES ---
   
    # Regions
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS regions (
        region_id INTEGER PRIMARY KEY,
        region_name TEXT
    )''')

    # Products
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        price REAL
    )''')

    # Customers (NEW!)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name TEXT,
        email TEXT,
        join_date DATE
    )''')

    # Sales (Updated with customer_id)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        sale_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        product_id INTEGER,
        region_id INTEGER,
        sale_date DATE,
        quantity INTEGER,
        total_amount REAL,
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY(product_id) REFERENCES products(product_id),
        FOREIGN KEY(region_id) REFERENCES regions(region_id)
    )''')

    # --- 2. POPULATE DATA ---
   
    # Regions
    regions = [(1, 'North America'), (2, 'Europe'), (3, 'Asia'), (4, 'South America')]
    cursor.executemany('INSERT INTO regions VALUES (?, ?)', regions)

    # Products
    products = [
        (1, 'Laptop Pro', 'Electronics', 1200.00),
        (2, 'Smart Phone X', 'Electronics', 800.00),
        (3, 'Ergo Chair', 'Furniture', 300.00),
        (4, 'Desk Lamp', 'Furniture', 45.00),
        (5, 'Gaming Monitor', 'Electronics', 400.00)
    ]
    cursor.executemany('INSERT INTO products VALUES (?, ?, ?, ?)', products)

    # Customers (Generate 50 random people)
    print("Generating customers...")
    customer_list = []
    for i in range(50):
        name = fake.name()
        email = fake.email()
        date = fake.date_between(start_date='-2y', end_date='today')
        customer_list.append((i+1, name, email, date))
   
    cursor.executemany('INSERT INTO customers VALUES (?, ?, ?, ?)', customer_list)

    # Sales
    print("Generating sales...")
    sales_data = []
    start_date = datetime(2023, 1, 1)
    for i in range(200):
        c_id = random.randint(1, 50) # Pick random customer
        p_id = random.randint(1, 5)  # Pick random product
        r_id = random.randint(1, 4)  # Pick random region
       
        date = start_date + timedelta(days=random.randint(0, 700))
        qty = random.randint(1, 5)
       
        # Calculate Total
        price = next(p[3] for p in products if p[0] == p_id)
        total = qty * price
       
        sales_data.append((i+1, c_id, p_id, r_id, date.strftime("%Y-%m-%d"), qty, total))

    cursor.executemany('INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?)', sales_data)
   
    conn.commit()
    conn.close()
    print("Database updated with Customers successfully.")

if __name__ == "__main__":
    init_db()