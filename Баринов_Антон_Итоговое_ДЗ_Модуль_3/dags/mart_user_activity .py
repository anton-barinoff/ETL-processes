from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

POSTGRES_CONN = "host=postgres dbname=airflow user=airflow password=airflow"

def create_mart_user_activity():
    """Создание витрины для аналитики по поведению пользователей (mart_user_activity)."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS mart_user_activity;")
    
    cursor.execute("""
        CREATE TABLE mart_user_activity AS
        SELECT user_id
            , COUNT(DISTINCT view_id) AS total_views
            , SUM(watch_duration_minutes) AS total_watch_time_minutes
            , AVG(watch_duration_minutes) AS avg_watch_duration
            , COUNT(DISTINCT movie_id) AS unique_movies_watched
            , MODE() WITHIN GROUP (ORDER BY genre) AS favorite_genre
            , SUM(CASE WHEN completed THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 AS completion_rate
            , MAX(start_time) AS last_activity_date
        FROM movie_views
        GROUP BY user_id
    ;
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM mart_user_activity;")
    count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT favorite_genre, COUNT(*) 
        FROM mart_user_activity 
        GROUP BY favorite_genre;
    """)
    
    print(f"Total users: {count}")
    cursor.close()
    conn.close()

def check_replication_status():
    """Проверка наличия данных в источнике."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    source = 'movie_views'
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
    dag_id='mart_user_activity',
    start_date=datetime(2026, 3, 8),
    schedule='@daily',
    catchup=False,
    tags=['mart', 'user_activity', 'analytics'],
) as dag:
    
    check_data = PythonOperator(
        task_id='check_replication_status',
        python_callable=check_replication_status
    )
    
    create_mart = PythonOperator(
        task_id='create_mart_user_activity',
        python_callable=create_mart_user_activity
    )
    
    check_data >> create_mart