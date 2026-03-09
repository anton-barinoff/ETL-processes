from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

POSTGRES_CONN = "host=postgres dbname=airflow user=airflow password=airflow"

def create_mart_payment_analysis():
    """Создание витрины для анализа платежей (mart_payment_analysis)."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS mart_payment_analysis;")
    
    cursor.execute("""
        CREATE TABLE mart_payment_analysis AS
        SELECT user_id
            , subscription_plan
            , COUNT(*) AS total_payments
            , SUM(amount) AS total_revenue
            , MIN(payment_date) AS first_payment
            , MAX(payment_date) AS last_payment
            , AVG(amount) AS avg_payment_amount
            , MODE() WITHIN GROUP (ORDER BY payment_method) AS preferred_payment_method
            , SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS successful_payments
        FROM user_payments
        GROUP BY user_id
            , subscription_plan
        ;
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM mart_payment_analysis;")
    count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT subscription_plan, COUNT(*) 
        FROM mart_payment_analysis 
        GROUP BY subscription_plan;
    """)
    plan_stats = cursor.fetchall()
    
    cursor.execute("""SELECT COUNT(DISTINCT user_id) FROM user_payments;""")
    unique_users = cursor.fetchone()[0]

    print(f"Total rows: {count}\n")
    print(f"Unique users: {unique_users}\n")
    print(f"Subscription plans distribution:\n")
    for plan, count in plan_stats:
        print(f"  {plan}: {count} users")

    cursor.close()
    conn.close()

def check_replication_status():
    """Проверка наличия данных в источнике."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    source = 'user_payments'
    source_ready = True
    cursor.execute(f"SELECT COUNT(*) FROM {source};")
    count = cursor.fetchone()[0]
    print(f"{source}: {count} rows")
    if count == 0:
        source_ready = False
    
    if not source_ready:
        raise Exception(f"Source table {source} is empty. Run replication DAG first.")
    
    cursor.close()
    conn.close()

    return source_ready

with DAG(
    dag_id='mart_payment_analysis',
    start_date=datetime(2026, 3, 8),
    schedule='@daily',
    catchup=False,
    tags=['mart', 'payments', 'analytics'],
) as dag:
    
    check_data = PythonOperator(
        task_id='check_replication_status',
        python_callable=check_replication_status
    )
    
    create_mart = PythonOperator(
        task_id='create_mart_payment_analysis',
        python_callable=create_mart_payment_analysis
    )
    
    check_data >> create_mart