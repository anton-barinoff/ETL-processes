from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import psycopg2
import os

def csv_to_parquet():
    spark = SparkSession.builder \
        .appName("CSV to Parquet") \
        .getOrCreate()
    
    df = spark.read.csv(
        "/opt/airflow/data/cleaned_data.csv",
        header=True,
        inferSchema=True
    )
    
    output_path = "/opt/airflow/data/one-time_load_output.parquet"
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"SAVED {df.count()} rows to Parquet")
    spark.stop()

def parquet_to_postgres():
    spark = SparkSession.builder \
        .appName("Parquet to Postgres") \
        .getOrCreate()
    
    df = spark.read.parquet("/opt/airflow/data/one-time_load_output.parquet")
    pandas_df = df.toPandas()
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS one_time_csv_import")

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS one_time_csv_import (
            {}
        )
    """
    
    columns_for_create = []
    for field in df.schema.fields:
        col_name = field.name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
        if str(field.dataType) == "StringType":
            col_type = "VARCHAR(255)"
        elif str(field.dataType) in ["IntegerType", "LongType"]:
            col_type = "INTEGER"
        elif str(field.dataType) in ["DoubleType", "FloatType"]:
            col_type = "FLOAT"
        elif str(field.dataType) == "TimestampType":
            col_type = "TIMESTAMP"
        else:
            col_type = "TEXT"
        
        columns_for_create.append(f"{col_name} {col_type}")
    
    cursor.execute(create_table_sql.format(", ".join(columns_for_create)))
    
    pandas_df = df.toPandas()
    
    rename_dict = {}
    for original_col in pandas_df.columns:
        new_col = original_col.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
        rename_dict[original_col] = new_col
    
    pandas_df = pandas_df.rename(columns=rename_dict)
    
    if not pandas_df.empty:
        columns_str = ", ".join([f'"{col}"' for col in pandas_df.columns])
        placeholders = ", ".join(["%s"] * len(pandas_df.columns))
        sql = f"INSERT INTO one_time_csv_import ({columns_str}) VALUES ({placeholders})"

        data_tuples = [tuple(row) for row in pandas_df.itertuples(index=False, name=None)]
        cursor.executemany(sql, data_tuples)
    
    conn.commit()
    print(f"LOADED {len(pandas_df)} rows to PostgreSQL")
    
    cursor.close()
    conn.close()
    spark.stop()

def validate_load():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM one_time_csv_import")
    count = cursor.fetchone()[0]
    
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'one_time_csv_import'")
    columns = [row[0] for row in cursor.fetchall()]
    
    print(f"Table_name: csv_import")
    print(f"Rows in table: {count}")
    print(f"Columns: {columns}")
    
    cursor.close()
    conn.close()

with DAG(
    dag_id="one-time_data_load",
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['csv', 'parquet', 'postgres', 'etl'],
    default_args={
        'retries': 1,
    }
) as dag:
    
    convert_csv = PythonOperator(
        task_id="convert_csv_to_parquet",
        python_callable=csv_to_parquet
    )
    
    load_parquet = PythonOperator(
        task_id="load_parquet_to_postgres",
        python_callable=parquet_to_postgres
    )
    
    validation = PythonOperator(
        task_id="validate_data_load",
        python_callable=validate_load
    )
    
    convert_csv >> load_parquet >> validation