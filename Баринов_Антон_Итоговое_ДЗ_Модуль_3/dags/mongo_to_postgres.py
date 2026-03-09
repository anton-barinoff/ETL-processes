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

def transform_movie_views(df):
    """Трансформация просмотров (movie_views)."""
    if df.empty:
        return df
    
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
    
    return df

def transform_user_payments(df):
    """Трансформация платежей (user_payments)."""
    if df.empty:
        return df
    
    df = df.drop_duplicates(subset=['payment_id'])
    df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    df['payment_method_category'] = df['payment_method'].map({
        'credit_card': 'card',
        'paypal': 'digital',
        'gift_card': 'voucher'
    }).fillna('other')
    
    df['loaded_at'] = datetime.now()
    return df

def transform_content_ratings(df):
    """Трансформация оценок (content_ratings)."""
    if df.empty:
        return df
    
    df = df.drop_duplicates(subset=['rating_id'])
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    
    df['loaded_at'] = datetime.now()
    return df

def transform_search_queries(df):
    """Трансформация поиска (search_queries)."""
    if df.empty:
        return df
    
    df = df.drop_duplicates(subset=['search_id'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['query_length'] = df['query'].str.len()
    df['filters_applied'] = df['filters_applied'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    
    df['loaded_at'] = datetime.now()
    return df

def transform_and_load(**context):
    """Трансформация всех данных и загрузка в PostgreSQL."""
    ti = context['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_from_mongo')
    
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
    
    conn.commit()
    
    transforms = {
        'movie_views': transform_movie_views,
        'user_payments': transform_user_payments,
        'content_ratings': transform_content_ratings,
        'search_queries': transform_search_queries
    }

    id_columns = {
        'movie_views': 'view_id',
        'user_payments': 'payment_id',
        'content_ratings': 'rating_id',
        'search_queries': 'search_id'
    }
    
    for col_name, transform_func in transforms.items():
        if col_name in raw_data and raw_data[col_name]:
            df = pd.DataFrame(raw_data[col_name])
            df = transform_func(df)
            
            if df.empty:
                continue
            cursor.execute(f"DELETE FROM {col_name} WHERE DATE(loaded_at) >= CURRENT_DATE")

            id_column = id_columns[col_name]
            
            success = 0
            for _, row in df.iterrows():
                row_dict = {k: v for k, v in row.items() 
                          if pd.notna(v) and v is not None}
                
                if not row_dict:
                    continue
                
                columns = list(row_dict.keys())
                values = [row_dict[col] for col in columns]
                
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                insert_sql = f"""
                    INSERT INTO {col_name} ({columns_str}) 
                    VALUES ({placeholders})
                    ON CONFLICT ({id_column}) DO NOTHING
                """
                
                try:
                    cursor.execute(insert_sql, values)
                    success += 1
                except Exception as e:
                    print(f"Error inserting into {col_name}: {e}")
                    raise
            
            conn.commit()
            print(f"Loaded {len(df)} rows into {col_name}")
    
    cursor.close()
    conn.close()

def validate_load(**context):
    """Валидация результатов."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cursor = conn.cursor()
    
    tables = ['movie_views', 'user_payments', 'content_ratings', 'search_queries']
    
    print("\n" + "="*50)
    print("LOAD SUMMARY")
    print("="*50)
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} rows")
    
    cursor.close()
    conn.close()

with DAG(
    dag_id='mongo_to_postgres',
    start_date=datetime(2026, 3, 7),
    schedule='@daily',
    catchup=False,
) as dag:
    
    extract = PythonOperator(
        task_id='extract_from_mongo',
        python_callable=extract_from_mongo
    )
    
    transform_load = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load
    )
    
    validate = PythonOperator(
        task_id='validate_load',
        python_callable=validate_load
    )
    
    extract >> transform_load >> validate