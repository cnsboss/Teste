from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
import json
import requests
import pandas as pd
import psycopg2
import os
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('etl_fake_store_with_ds_teste2', description='Teste', schedule_interval=None, 
          catchup=False, default_args=default_args, default_view='graph'
)

def load_dim_product(**kwargs):
    URL_PRODUCTS = "https://fakestoreapi.com/products"
    # try:
    response = requests.get(URL_PRODUCTS)
    response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
    products = response.json()  # Lista de dicionários
    df_products = pd.DataFrame(products)
    
    pg_hook = PostgresHook(postgres_conn_id='ds_teste')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    insert_dimension_product = """
        INSERT INTO public.dim_product(product_id, product_name, category, price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """
    
    for _, row in df_products.iterrows():
        data = (row['id'], row['title'], row['category'], row['price'])
        cursor.execute(insert_dimension_product, data)
    
    conn.commit()
    cursor.close()
    conn.close()
    # except requests.exceptions.RequestException as e:
    #     raise Exception(f"Erro na requisição para API de produtos: {e}")
    # except Exception as e:
    #     raise Exception(f"Erro ao carregar dados de produtos: {e}")

def load_dim_customer(**kwargs):    
    URL_USERS = "https://fakestoreapi.com/users"
    # try:
    response = requests.get(URL_USERS)
    response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
    users = response.json()  # Lista de dicionários
    df_users = pd.DataFrame(users)
    
    pg_hook = PostgresHook(postgres_conn_id='ds_teste')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    insert_dimension_customer = """
        INSERT INTO public.dim_customer(customer_id, name, email, registration_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """
    
    for _, row in df_users.iterrows():
        data = (row['id'], row['username'], row['email'], datetime.now().date())
        cursor.execute(insert_dimension_customer, data)
    
    conn.commit()
    cursor.close()
    conn.close()
    # except requests.exceptions.RequestException as e:
    #     raise Exception(f"Erro na requisição para API de usuários: {e}")
    # except Exception as e:
    #     raise Exception(f"Erro ao carregar dados de clientes: {e}")

def load_fact_table():
    # try:
    pg_hook = PostgresHook(postgres_conn_id='ds_teste')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_fact = """
        INSERT INTO fact_sales (order_id, customer_id, product_id, date_id, quantity, total_value)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    with open("/opt/airflow/data/orders.json", 'r') as file:
        orders = json.load(file)

    for order in orders:
        data = (order['order_id'], order['customer_id'], order['product_id'], "2025-02-20", order['quantity'], order['total_value'])
        cursor.execute(insert_fact, data)

    conn.commit()
    cursor.close()
    conn.close()
    # except FileNotFoundError:
    #     raise Exception(f"Arquivo de pedidos não encontrado: {file_path}")
    # except json.JSONDecodeError:
    #     raise Exception("Erro ao decodificar o arquivo JSON de pedidos")
    # except Exception as e:
    #     raise Exception(f"Erro ao carregar dados de vendas: {e}")

load_dim_product_task = PythonOperator(
    task_id='load_dim_product',
    python_callable=load_dim_product,
    dag=dag
)

load_dim_customer_task = PythonOperator(
    task_id='load_dim_customer',
    python_callable=load_dim_customer,
    dag=dag
)

load_fact_table_task = PythonOperator(
    task_id='load_fact_table',
    python_callable=load_fact_table,
    dag=dag
)

[load_dim_product_task, load_dim_customer_task] >> load_fact_table_task