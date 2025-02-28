import json
import requests
import pandas as pd
import psycopg2
from datetime import datetime

# URLs dos endpoints da Fake Store API
URL_PRODUCTS = "https://fakestoreapi.com/products"
URL_USERS    = "https://fakestoreapi.com/users"

# Busca os dados
products = requests.get(URL_PRODUCTS).json()
users = requests.get(URL_USERS).json()

df_products = pd.DataFrame(products)
df_users = pd.DataFrame(users)

conn = psycopg2.connect(
    host="localhost", 
    database="Teste",
    user="postgres",
    password="123456"
)

cursor = conn.cursor()

insert_dimension_product = """
    INSERT INTO public.dim_product(product_id, product_name, category, price)
	VALUES (%s, %s, %s, %s)
    ON CONFLICT (product_id) DO NOTHING;
"""

for index, product in df_products.iterrows():
    data = (product['id'], product['title'], product['category'], product['price'])
    cursor.execute(insert_dimension_product, data)

insert_dimension_customer = """
    INSERT INTO public.dim_customer(customer_id, name, email, registration_date)
	VALUES (%s, %s, %s, %s)
    ON CONFLICT (product_id) DO NOTHING;
"""

for index, customer in df_users.iterrows():
    data = (customer['id'], customer['username'], customer['email'], datetime.now())
    cursor.execute(insert_dimension_customer, data)

insert_fact = """
    INSERT INTO fact_sales (order_id, customer_id, product_id, date_id, quantity, total_value)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

orders = []

with open('orders.json', 'r') as file:
    orders = json.load(file)

for order in orders:
    data = (order['order_id'], order['customer_id'], order['product_id'], order['order_date'], order['quantity'], order['total_value'])
    cursor.execute(insert_fact, data)

conn.commit()

print("DONE!")