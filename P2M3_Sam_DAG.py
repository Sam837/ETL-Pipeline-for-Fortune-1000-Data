# import library
import pandas as pd
import numpy as np
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from elasticsearch import Elasticsearch

# defaults arguments
default_args = {
    'owner':'Sam',
    'retries':None,
    'start_date': datetime(2024, 11, 1)
}

# task 1: extract
def extract_data(**context):
    # create connection to postgres
    source_hook = PostgresHook(postgres_conn_id='postgres_airflow')
    source_conn = source_hook.get_conn()

    # read data using pandas
    raw_data = pd.read_sql('SELECT * FROM fortune_1000', source_conn)

    # testing
    #print(raw_data)

    # x_com / save temporary
    temp_path = '/tmp/P2M3_Sam_data_raw.csv'
    raw_data.to_csv(temp_path, index=False)

    context['ti'].xcom_push(key='path_raw_data', value=temp_path)

# task 2: transform
def transform_data(**context):
    # pull raw data
    path_raw_data = context['ti'].xcom_pull(task_ids ='extract_data', key='path_raw_data')

    # read data
    raw_data = pd.read_csv(path_raw_data)

    #print(raw_data)

    # cleaning process
    # note: since on the P2M3_Sam_data_visualization.ipynb, we know that there's no duplicates, we are not gonna process it.
    clean_data = raw_data.dropna()
    clean_data['num_of_employees'] = clean_data['num_of_employees'].astype(int)
    # calculate median of prev_rank (excluding empty strings/spaces)
    median_prev_rank = clean_data[clean_data['prev_rank'].str.strip() != '']['prev_rank'].astype(float).median()
    # replace empty strings/spaces with median value
    clean_data['prev_rank'] = clean_data['prev_rank'].apply(
        lambda x: median_prev_rank if str(x).strip() == '' else x
    )
    
    clean_data['prev_rank'] = pd.to_numeric(clean_data['prev_rank'], errors='coerce').astype(float).astype(int)
    clean_data['market_cap'] = pd.to_numeric(clean_data['market_cap'], errors='coerce').astype(float)
    print(clean_data.info())

    # x_com / save temporary
    temp_path = '/tmp/P2M3_Sam_data_clean.csv'
    clean_data.to_csv(temp_path, index=False)

    context['ti'].xcom_push(key='path_clean_data', value=temp_path)

    # Task 3: load data to Elasticsearch
def load_data(**context):

    path_clean_data = context['ti'].xcom_pull(task_ids='transform_data', key='path_clean_data')
    clean_data = pd.read_csv(path_clean_data)
    print(clean_data.columns)
    records = clean_data.to_dict('records')
    es = Elasticsearch(hosts=['http://elasticsearch:9200'])
    
    index_name = 'fortune'
    
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    
    for i, record in enumerate(records):
        try:
            es.index(index=index_name, id=i+1, body=record)
        except Exception as e:
            print(f"Failed to index document {i}: {str(e)}")
    
    print(f"Successfully loaded {len(records)} documents to Elasticsearch index '{index_name}'")

with DAG (
    'P2M3_Sam_DAG',
    description = 'pipeline milestone 3',
    default_args = default_args,
    schedule_interval = '*/10 * * * *',
    catchup = False
) as dag: 
    
    # task 1: extract
    extract = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data,
        provide_context = True
    )

    # task 2: transform
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context = True
    )
    
    # task 3: load
    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

extract >> transform >> load