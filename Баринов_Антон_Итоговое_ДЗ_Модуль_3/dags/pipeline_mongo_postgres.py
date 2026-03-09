from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
import pandas as pd
import json

MONGO_URI = "mongodb://mongoadmin:admin123@mongodb:27017/"
POSTGRES_CONN = "host=postgres dbname=airflow user=airflow password=airflow"

def extract_from_mongo(**context):
    """Извлечение данных из MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client['etl_db']
    
    data = {
        'movie_views': list(db.movie_views.find({}, {'_id': 0})),
        'user_payments': list(db.user_payments.find({}, {'_id': 0})),
        'content_ratings': list(db.content_ratings.find({}, {'_id': 0})),
        'search_queries': list(db.search_queries.find({}, {'_id': 0}))
    }
    
    context['ti'].xcom_push(key='raw_data', value=data)
    
    print(f"Extracted: { {k: len(v) for k, v in data.items()} }")
    
    client.close()

def transform_movie_views(**context):
    """Трансформация просмотров (movie_views)."""
    ti = context['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_from_mongo')

    if not raw_data or 'movie_views' not in raw_data:
        return 0
    
    df = pd.DataFrame(raw_data['movie_views'])
    if df.empty:
        return 0
    
    df = df.drop_duplicates(subset=['view_id'])
    
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
    df['watch_duration_minutes'] = pd.to_numeric(df['watch_duration_minutes'], errors='coerce')
    
    if 'device' in df.columns:
        df['device_type'] = df['device'].apply(lambda x: x.get('type') if isinstance(x, dict) else None)
        df['device_os'] = df['device'].apply(lambda x: x.get('os') if isinstance(x, dict) else None)
        df.drop(columns=['device'], inplace=True)
    
    if 'interactions' in df.columns:
        df['interactions'] = df['interactions'].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else None
        )
    df['loaded_at'] = datetime.now()
    df['source'] = 'mongodb'
    
    records = df.to_dict('records')
    for record in records:
        for key, value in record.items():
            if hasattr(value, 'isoformat'):
                record[key] = value.isoformat()
            elif pd.isna(value):
                record[key] = None

    ti.xcom_push(key='transformed_movie_views', value=records)

def transform_user_payments(**context):
    """Трансформация платежей (user_payments)."""
    ti = context['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_from_mongo')

    if not raw_data or 'user_payments' not in raw_data:
        return 0
    
    df = pd.DataFrame(raw_data['user_payments'])

    if df.empty:
        return 0
    
    df = df.drop_duplicates(subset=['payment_id'])

    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    df['payment_method_category'] = df['payment_method'].map({
        'credit_card': 'card',
        'paypal': 'digital',
        'gift_card': 'voucher'
    }).fillna('other')
    
    df['loaded_at'] = datetime.now()
    df['source'] = 'mongodb'

    records = df.to_dict('records')
    for record in records:
        for key, value in record.items():
            if hasattr(value, 'isoformat'):
                record[key] = value.isoformat()
            elif pd.isna(value):
                record[key] = None

    ti.xcom_push(key='transformed_user_payments', value=records)

def transform_content_ratings(**context):
    """Трансформация оценок (content_ratings)."""
    ti = context['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_from_mongo')

    if not raw_data or 'content_ratings' not in raw_data:
        return 0
    
    df = pd.DataFrame(raw_data['content_ratings'])

    if df.empty:
        return 0
    
    df = df.drop_duplicates(subset=['rating_id'])

    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    
    df['loaded_at'] = datetime.now()
    df['source'] = 'mongodb'

    records = df.to_dict('records')
    for record in records:
        for key, value in record.items():
            if hasattr(value, 'isoformat'):
                record[key] = value.isoformat()
            elif pd.isna(value):
                record[key] = None

    ti.xcom_push(key='transformed_content_ratings', value=records)

def transform_search_queries(**context):
    """Трансформация поиска (search_queries)."""
    ti = context['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_from_mongo')

    if not raw_data or 'search_queries' not in raw_data:
        return 0
    
    df = pd.DataFrame(raw_data['search_queries'])

    if df.empty:
        return 0
    
    df = df.drop_duplicates(subset=['search_id'])

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['query_length'] = df['query'].str.len()
    df['filters_applied'] = df['filters_applied'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    
    df['loaded_at'] = datetime.now()
    df['source'] = 'mongodb'

    records = df.to_dict('records')
    for record in records:
        for key, value in record.items():
            if hasattr(value, 'isoformat'):
                record[key] = value.isoformat()
            elif pd.isna(value):
                record[key] = None

    ti.xcom_push(key='transformed_search_queries', value=records)

def load_movie_views(**context):
    """Загрузка просмотров (movie_views) в PostgreSQL."""
    ti = context['ti']
    data = ti.xcom_pull(key='transformed_movie_views', task_ids='transform_movie_views')
    
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_views (
            view_id TEXT PRIMARY KEY,
            user_id TEXT,
            movie_id TEXT,
            movie_title TEXT,
            genre TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            watch_duration_minutes INTEGER,
            completed BOOLEAN,
            device_type TEXT,
            device_os TEXT,
            interactions JSONB,
            loaded_at TIMESTAMP,
            source TEXT
        )
    """)
    cursor.execute("DELETE FROM movie_views WHERE DATE(loaded_at) >= CURRENT_DATE")

    df = pd.DataFrame(data)
    if df.empty:
        return 0
    
    success = 0
    for _, row in df.iterrows():
        row_dict = {k: v for k, v in row.items() if pd.notna(v)}
        if not row_dict:
            continue
        columns = list(row_dict.keys())
        values = [row_dict[col] for col in columns]
        placeholders = ', '.join(['%s'] * len(columns)) 
        columns_str = ', '.join(columns)

        insert_sql = f"""
            INSERT INTO movie_views ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (view_id) DO NOTHING
        """
        cursor.execute(insert_sql, values)
        success += 1        
    conn.commit()
    cursor.close()
    conn.close()
    
    return success

def load_user_payments(**context):
    """Загрузка платежей (user_payments) в PostgreSQL."""
    ti = context['ti']
    data = ti.xcom_pull(key='transformed_user_payments', task_ids='transform_user_payments')
    
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_payments (
            payment_id TEXT PRIMARY KEY,
            user_id TEXT,
            payment_date TIMESTAMP,
            amount DECIMAL(10,2),
            currency TEXT,
            payment_method TEXT,
            payment_method_category TEXT,
            subscription_plan TEXT,
            status TEXT,
            next_billing_date TIMESTAMP,
            promo_code_applied TEXT,
            loaded_at TIMESTAMP,
            source TEXT
        )
    """)
    cursor.execute("DELETE FROM user_payments WHERE DATE(loaded_at) >= CURRENT_DATE")

    df = pd.DataFrame(data)
    if df.empty:
        return 0

    success = 0
    for _, row in df.iterrows():
        row_dict = {k: v for k, v in row.items() if pd.notna(v)}
        if not row_dict:
            continue
        columns = list(row_dict.keys())
        values = [row_dict[col] for col in columns]
        placeholders = ', '.join(['%s'] * len(columns)) 
        columns_str = ', '.join(columns)

        insert_sql = f"""
            INSERT INTO user_payments ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (payment_id) DO NOTHING
        """
        cursor.execute(insert_sql, values)
        success += 1
    conn.commit()
    cursor.close()
    conn.close()
    
    return success

def load_content_ratings(**context):
    """Загрузка оценок (content_ratings) в PostgreSQL."""
    ti = context['ti']
    data = ti.xcom_pull(key='transformed_content_ratings', task_ids='transform_content_ratings')
    
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS content_ratings (
            rating_id TEXT PRIMARY KEY,
            user_id TEXT,
            movie_id TEXT,
            movie_title TEXT,
            rating INTEGER,
            review_text TEXT,
            created_at TIMESTAMP,
            helpful_count INTEGER,
            reports_count INTEGER DEFAULT 0,
            moderation_flag BOOLEAN DEFAULT FALSE,
            loaded_at TIMESTAMP,
            source TEXT
        )
    """)
    cursor.execute("DELETE FROM content_ratings WHERE DATE(loaded_at) >= CURRENT_DATE")

    df = pd.DataFrame(data)
    if df.empty:
        return 0
    
    success = 0
    for _, row in df.iterrows():
        row_dict = {k: v for k, v in row.items() if pd.notna(v)}
        if not row_dict:
            continue
        columns = list(row_dict.keys())
        values = [row_dict[col] for col in columns]
        placeholders = ', '.join(['%s'] * len(columns)) 
        columns_str = ', '.join(columns)

        insert_sql = f"""
            INSERT INTO content_ratings ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (rating_id) DO NOTHING
        """
        cursor.execute(insert_sql, values)
        success += 1
    conn.commit()
    cursor.close()
    conn.close()
    
    return success

def load_search_queries(**context):
    """Загрузка поисковых запросов (search_queries) в PostgreSQL."""
    ti = context['ti']
    data = ti.xcom_pull(key='transformed_search_queries', task_ids='transform_search_queries')
    
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS search_queries (
            search_id TEXT PRIMARY KEY,
            user_id TEXT,
            query TEXT,
            query_length INTEGER,
            timestamp TIMESTAMP,
            filters_applied JSONB,
            results_count INTEGER,
            clicked_movie_id TEXT,
            session_id TEXT,
            loaded_at TIMESTAMP,
            source TEXT
        )
    """)
    cursor.execute("DELETE FROM search_queries WHERE DATE(loaded_at) >= CURRENT_DATE")

    df = pd.DataFrame(data)
    if df.empty:
        return 0
    
    success = 0
    for _, row in df.iterrows():
        row_dict = {k: v for k, v in row.items() if pd.notna(v)}
        if not row_dict:
            continue
        columns = list(row_dict.keys())
        values = [row_dict[col] for col in columns]
        placeholders = ', '.join(['%s'] * len(columns)) 
        columns_str = ', '.join(columns)

        insert_sql = f"""
            INSERT INTO search_queries ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (search_id) DO NOTHING
        """
        cursor.execute(insert_sql, values)
        success += 1
    conn.commit()
    cursor.close()
    conn.close()

    return success

def validate_loads(**context):
    """Валидация всех результирующих таблиц."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    tables = ['movie_views', 'user_payments', 'content_ratings', 'search_queries']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} rows")
    
    cursor.close()
    conn.close()

with DAG(
    dag_id='pipeline_mongo_postgres',
    start_date=datetime(2026, 3, 7),
    schedule='@daily',
    catchup=False,
) as dag:
    
    extract = PythonOperator(
        task_id='extract_from_mongo',
        python_callable=extract_from_mongo
    )
    
    transform_movies = PythonOperator(
        task_id='transform_movie_views',
        python_callable=transform_movie_views
    )

    transform_payments = PythonOperator(
        task_id='transform_user_payments',
        python_callable=transform_user_payments
    )
    
    transform_ratings = PythonOperator(
        task_id='transform_content_ratings',
        python_callable=transform_content_ratings
    )

    transform_searches = PythonOperator(
        task_id='transform_search_queries',
        python_callable=transform_search_queries
    )

    load_movies = PythonOperator(
        task_id='load_movie_views',
        python_callable=load_movie_views
    )

    load_payments = PythonOperator(
        task_id='load_user_payments',
        python_callable=load_user_payments
    )

    load_ratings = PythonOperator(
        task_id='load_content_ratings',
        python_callable=load_content_ratings
    )

    load_searches = PythonOperator(
        task_id='load_search_queries',
        python_callable=load_search_queries
    )   

    validate = PythonOperator(
        task_id='validate_loads',
        python_callable=validate_loads
    )
    
    extract >> [transform_movies, transform_payments, transform_ratings, transform_searches]

    transform_movies >> load_movies
    transform_payments >> load_payments
    transform_ratings >> load_ratings
    transform_searches >> load_searches
               
    [load_movies, load_payments, load_ratings, load_searches] >> validate