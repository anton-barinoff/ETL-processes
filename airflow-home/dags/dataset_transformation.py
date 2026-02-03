from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd


def extract_data(**context):
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    temp_path = '/tmp/iot_data_raw.csv'
    df.to_csv(temp_path, index=False)

    print(f"LOADED: {len(df)} rows")
    return temp_path

def find_extreme_temps(**context):
    ti = context['ti']
    input_path = ti.xcom_pull(task_ids='extract_task')
    
    df = pd.read_csv(input_path)
    
    # Вычислите 5 самых жарких и самых холодных дней за год.
    hot_days_top  = df.nlargest(5, 'temp')
    cold_days_top = df.nsmallest(5, 'temp')
    extreme_temps = pd.concat([hot_days_top, cold_days_top])
    extreme_temps.to_csv('/opt/airflow/data/extreme_temps.csv', index=False)

    return "SUCCESS: File saved"

def filter_and_clean(**context):
    ti = context['ti']
    input_path = ti.xcom_pull(task_ids='extract_task')
    
    df = pd.read_csv(input_path)
    
    # Отфильтруйте out/in = in.
    df = df[df['out/in'] == 'In']
    
    # Поле noted_date переведите в формат ‘yyyy-MM-dd’ с типом данных date
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    df['noted_date'] = df['noted_date'].dt.date
    
    # Удаляем выбросы
    lower_bound = df['temp'].quantile(0.05)
    upper_bound = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= lower_bound) & (df['temp'] <= upper_bound)]
 
    df.to_csv('/opt/airflow/data/cleaned_data.csv', index=False)
    return '/opt/airflow//data/cleaned_data.csv'

with DAG(
    dag_id='dataset_transformation',
    schedule='@daily',
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['dataset', 'pandas', 'etl'],
    default_args={
        'retries': 1,
    }
) as dag:
    start = EmptyOperator(task_id='start')
    extract_task = PythonOperator(task_id='extract_task', python_callable=extract_data)
    end = EmptyOperator(task_id='end')
    
    find_extremes_task = PythonOperator(
        task_id='find_extremes_task',
        python_callable=find_extreme_temps,
    )
    
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=filter_and_clean,
    )
    
    start >> extract_task >> [find_extremes_task, transform_task] >> end
