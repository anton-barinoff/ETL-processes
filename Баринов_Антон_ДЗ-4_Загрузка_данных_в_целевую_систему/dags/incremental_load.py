from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
import psycopg2

SOURCE_CSV = "/opt/airflow/data/cleaned_data.csv"
TARGET_TABLE = "one_time_csv_import"
DATE_COLUMN = "noted_date"

def get_last_loaded_date(**context):
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_name = '{TARGET_TABLE}' 
            AND column_name = '{DATE_COLUMN}'
        )
    """)
    
    if not cursor.fetchone()[0]:
        cursor.execute(f"""
            ALTER TABLE {TARGET_TABLE} 
            ADD COLUMN IF NOT EXISTS {DATE_COLUMN} DATE DEFAULT CURRENT_DATE
        """)
        conn.commit()
        last_date = datetime.now().date() - timedelta(days=365)  # давно, чтобы взять все
    else:
        cursor.execute(f"SELECT MAX({DATE_COLUMN}) FROM {TARGET_TABLE}")
        last_date = cursor.fetchone()[0]
        if last_date is None:
            last_date = datetime.now().date() - timedelta(days=365)
    
    cursor.close()
    conn.close()
    print(f"Last loaded date: {last_date}")
    
    context['ti'].xcom_push(key='last_date', value=str(last_date))
    return last_date

def load_new_data(**context):
    ti = context['ti']

    last_date_str = ti.xcom_pull(task_ids='get_last_loaded_date', key='last_date')
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    
    spark = SparkSession.builder \
        .appName("Incremental Load") \
        .getOrCreate()
    
    df = spark.read.csv(SOURCE_CSV, header=True, inferSchema=True)
    
    df.createOrReplaceTempView("source_data")
    new_data = spark.sql(f"""
        SELECT * 
        FROM source_data 
        WHERE {DATE_COLUMN} > '{last_date}'
    """)
    
    new_count = new_data.count()
    print(f"New rows since {last_date}: {new_count}")
    
    if new_count == 0:
        spark.stop()
        return 0
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    pandas_df = new_data.toPandas()
    
    rename_dict = {}
    for original_col in pandas_df.columns:
        new_col = original_col.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
        rename_dict[original_col] = new_col
    pandas_df = pandas_df.rename(columns=rename_dict)
    
    columns_str = ", ".join([f'"{col}"' for col in pandas_df.columns])
    placeholders = ", ".join(["%s"] * len(pandas_df.columns))
    sql = f"INSERT INTO {TARGET_TABLE} ({columns_str}) VALUES ({placeholders})"
    
    data_tuples = [tuple(row) for row in pandas_df.itertuples(index=False, name=None)]
    cursor.executemany(sql, data_tuples)
    
    conn.commit()
    cursor.close()
    conn.close()
    spark.stop()
    
    print(f"Inserted {new_count} new rows into {TARGET_TABLE}")
    
    ti.xcom_push(key='new_rows_count', value=new_count)
    return f"Loaded {new_count} rows"

def validate_incremental(**context):
    ti = context['ti']
    new_count = ti.xcom_pull(task_ids='load_new_data', key='new_rows_count')
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
    total = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT MIN({DATE_COLUMN}), MAX({DATE_COLUMN}) FROM {TARGET_TABLE}")
    min_date, max_date = cursor.fetchone()
    
    print(f"Table_name: {TARGET_TABLE}")
    print(f"Total rows: {total}")
    print(f"Date range: {min_date} to {max_date}")
    print(f"New rows added in this run: {new_count or 0}")
    
    cursor.close()
    conn.close()

with DAG(
    dag_id="incremental_load",
    schedule='@daily',
    start_date=datetime(2026, 2, 8),
    catchup=False,
    tags=['csv', 'parquet', 'postgres', 'etl'],
    default_args={
        'retries': 1,
    }
) as dag:
    
    get_date_task = PythonOperator(
        task_id="get_last_loaded_date",
        python_callable=get_last_loaded_date
    )
    
    load_task = PythonOperator(
        task_id="load_new_data",
        python_callable=load_new_data,
    )
    
    validate_task = PythonOperator(
        task_id="validate_incremental_load",
        python_callable=validate_incremental,
    )
    
    get_date_task >> load_task >> validate_task