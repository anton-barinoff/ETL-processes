from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import psycopg2
import requests
import pandas as pd

def extract_data(**context):
    url = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
    response = requests.get(url)
    data = response.json()
    pets_data = data.get("pets", [])
    print(f"EXTRACTED: {len(pets_data)} JSON records")
    
    context['ti'].xcom_push(key='raw_pets_data', value=pets_data)
    return pets_data

def transform_data(**context):
    ti = context['ti']
    pets_data = ti.xcom_pull(task_ids='extract_json', key='raw_pets_data')
    
    if not pets_data:
        raise ValueError("ERROR: No data for transformation")
    
    df = pd.DataFrame(pets_data)
    df['favFoods'] = df['favFoods'].apply(
        lambda x: ', '.join(x) if isinstance(x, list) else str(x)
    )

    records = []
    for _, row in df.iterrows():
        record = (
            row.get('name'),
            row.get('species'),
            row.get('favFoods'),
            row.get('birthYear'),
            row.get('photo')
        )
        records.append(record)
    
    print(f"TRANSFORMED: {len(records)} records")
    
    context['ti'].xcom_push(key='transformed_pets_data', value=records)
    return records

def load_data(**context):
    ti = context['ti']
    records = ti.xcom_pull(task_ids='transform_data', key='transformed_pets_data')
    
    if not records:
        raise ValueError("ERROR: No data to load")
    
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow',
        port=5432
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pets_data (
            id SERIAL PRIMARY KEY
            , name VARCHAR(50)
            , species VARCHAR(50)
            , favFoods TEXT
            , birthYear INTEGER
            , photo TEXT
            , loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            , UNIQUE(name, species, birthYear)
        )
    """)
    conn.commit()
    
    insert_sql = """
        INSERT INTO pets_data (name, species, favFoods, birthYear, photo)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (name, species, birthYear) 
        DO UPDATE SET
            favFoods = EXCLUDED.favFoods,
            photo = EXCLUDED.photo,
            loaded_at = CURRENT_TIMESTAMP
    """
    
    cursor.executemany(insert_sql, records)
    conn.commit()
    
    cursor.close()
    conn.close()
    
    return f"LOADED: {cursor.rowcount} rows"

with DAG(
    dag_id='json_etl',
    schedule='@daily',
    start_date=datetime(2026, 1, 20),
    catchup=False,
    tags=['json', 'postgres', 'etl'],
    default_args={
        'retries': 1,
    }
) as dag:
    extract_task = PythonOperator(
        task_id='extract_json',
        python_callable=extract_data,
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_data,
    )
    
    extract_task >> transform_task >> load_task