from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

POSTGRES_CONN = "host=postgres dbname=airflow user=airflow password=airflow"

def create_mart_content_performance():
    """Создание витрины для аналитики по фильмам (mart_content_performance)."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS mart_content_performance;")
    
    cursor.execute("""
        CREATE TABLE mart_content_performance AS
        SELECT mv.movie_id
            , mv.movie_title
            , mv.genre
            , COUNT(DISTINCT mv.view_id) AS total_views
            , COUNT(DISTINCT mv.user_id) AS unique_viewers
            , AVG(mv.watch_duration_minutes) as avg_watch_duration
            , AVG(cr.rating) as avg_rating
            , COUNT(DISTINCT cr.rating_id) as total_ratings
            , COUNT(DISTINCT sq.search_id) as search_appearances
            , COUNT(DISTINCT CASE WHEN sq.clicked_movie_id = mv.movie_id THEN sq.search_id END)::FLOAT / 
                NULLIF(COUNT(DISTINCT sq.search_id), 0) * 100 AS search_to_view_rate
        FROM movie_views mv
        LEFT JOIN content_ratings cr 
            ON mv.movie_id = cr.movie_id
        LEFT JOIN search_queries sq 
            ON mv.movie_id = sq.clicked_movie_id
        GROUP BY mv.movie_id
            , mv.movie_title
            , mv.genre
        ;
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM mart_content_performance;")
    count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT genre, COUNT(*), AVG(avg_rating) 
        FROM mart_content_performance 
        GROUP BY genre;
    """)
    
    print(f"Total movies: {count}")
    cursor.close()
    conn.close()

def check_replication_status():
    """Проверка наличия данных в источнике."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    source = ['movie_views', 'content_ratings', 'search_queries']
    source_ready = True
    for table in source:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} rows")
        if count == 0:
            source_ready = False
        if not source_ready:
            raise Exception(f"Source table {table} is empty. Run replication DAG first.")
    
    cursor.close()
    conn.close()

    return source_ready

with DAG(
    dag_id='mart_content_performance',
    start_date=datetime(2026, 3, 8),
    schedule='@daily',
    catchup=False,
    tags=['mart', 'content', 'analytics'],
) as dag:
    
    check_data = PythonOperator(
        task_id='check_replication_status',
        python_callable=check_replication_status
    )
    
    create_mart = PythonOperator(
        task_id='create_mart_content_performance',
        python_callable=create_mart_content_performance
    )
    
    check_data >> create_mart