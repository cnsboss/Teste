import psycopg2
import json

with open('orders.json', 'r') as file:
    orders = json.load(file)

for order in orders:
    order['total_value'] = order['quantity'] * order['price']
    # Converter order_date para datetime, extrair dia, mês, ano, weekday, etc.

conn = psycopg2.connect(
    host="localhost",
    database="Teste",
    user="postgres",
    password="mitnick"
)
cursor = conn.cursor()

# Exemplo de inserção para fact_sales:
insert_fact = """
INSERT INTO fact_sales (order_id, customer_id, product_id, date_id, quantity, total_value)
VALUES (%s, %s, %s, %s, %s, %s)
"""
data = (order['order_id'], order['customer_id'], order['product_id'], order['order_date'], order['quantity'], order['total_value'])
cursor.execute(insert_fact, data)
conn.commit()
