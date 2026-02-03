from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import psycopg2
import requests
import xml.etree.ElementTree as ET

def extract_and_parse_xml(**context):
    url = "https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml"
    
    try:
        print(f"Fetching XML from: {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        xml_content = response.content
        print(f"EXTRACTED: {len(xml_content)} bytes")
        
        root = ET.fromstring(xml_content)
        
        foods = []
        
        for food_elem in root.findall('food'):
            name = food_elem.find('name').text if food_elem.find('name') is not None else None
            mfr = food_elem.find('mfr').text if food_elem.find('mfr') is not None else None

            serving_elem = food_elem.find('serving')
            serving_value = serving_elem.text if serving_elem is not None else None
            serving_units = serving_elem.get('units') if serving_elem is not None else None

            calories_elem = food_elem.find('calories')
            calories_total = calories_elem.get('total') if calories_elem is not None else None
            calories_fat = calories_elem.get('fat') if calories_elem is not None else None
            
            total_fat = food_elem.find('total-fat').text if food_elem.find('total-fat') is not None else None
            saturated_fat = food_elem.find('saturated-fat').text if food_elem.find('saturated-fat') is not None else None
            cholesterol = food_elem.find('cholesterol').text if food_elem.find('cholesterol') is not None else None
            sodium = food_elem.find('sodium').text if food_elem.find('sodium') is not None else None
            carb = food_elem.find('carb').text if food_elem.find('carb') is not None else None
            fiber = food_elem.find('fiber').text if food_elem.find('fiber') is not None else None
            protein = food_elem.find('protein').text if food_elem.find('protein') is not None else None
            
            vitamins_elem = food_elem.find('vitamins')
            vitamin_a = vitamins_elem.find('a').text if vitamins_elem is not None and vitamins_elem.find('a') is not None else None
            vitamin_c = vitamins_elem.find('c').text if vitamins_elem is not None and vitamins_elem.find('c') is not None else None
            
            minerals_elem = food_elem.find('minerals')
            calcium = minerals_elem.find('ca').text if minerals_elem is not None and minerals_elem.find('ca') is not None else None
            iron = minerals_elem.find('fe').text if minerals_elem is not None and minerals_elem.find('fe') is not None else None
            
            food_data = {
                'name': name,
                'manufacturer': mfr,
                'serving_value': serving_value,
                'serving_units': serving_units,
                'calories_total': calories_total,
                'calories_fat': calories_fat,
                'total_fat': total_fat,
                'saturated_fat': saturated_fat,
                'cholesterol': cholesterol,
                'sodium': sodium,
                'carbohydrates': carb,
                'fiber': fiber,
                'protein': protein,
                'vitamin_a': vitamin_a,
                'vitamin_c': vitamin_c,
                'calcium': calcium,
                'iron': iron
            }
            
            foods.append(food_data)
        
        context['ti'].xcom_push(key='parsed_foods', value=foods)
        return (f"PARSED: {len(foods)} food records")
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise

def transform_data(**context):
    ti = context['ti']
    foods = ti.xcom_pull(task_ids='extract_and_parse_xml', key='parsed_foods')
    
    if not foods:
        raise ValueError("ERROR: No data for transformation")

    transformed_records = []
    
    for food in foods:
        def to_float(value):
            try:
                return float(value) if value else None
            except (ValueError, TypeError):
                return None
        
        def to_int(value):
            try:
                return int(float(value)) if value else None
            except (ValueError, TypeError):
                return None
        
        record = (
            food['name'],
            food['manufacturer'],
            to_float(food['serving_value']),
            food['serving_units'],
            to_int(food['calories_total']),
            to_int(food['calories_fat']),
            to_float(food['total_fat']),
            to_float(food['saturated_fat']),
            to_int(food['cholesterol']),
            to_int(food['sodium']),
            to_float(food['carbohydrates']),
            to_float(food['fiber']),
            to_float(food['protein']),
            to_int(food['vitamin_a']),
            to_int(food['vitamin_c']),
            to_int(food['calcium']),
            to_int(food['iron'])
        )
        
        transformed_records.append(record)

    context['ti'].xcom_push(key='transformed_foods', value=transformed_records)
    return (f"TRANSFORMED: {len(transformed_records)} records")

def load_data(**context):
    ti = context['ti']
    records = ti.xcom_pull(task_ids='transform_data', key='transformed_foods')
    
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
    
    cursor.execute("DROP TABLE IF EXISTS nutrition_data;")
    
    cursor.execute("""
        CREATE TABLE nutrition_data (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            manufacturer TEXT,
            serving_value DECIMAL(10,2),
            serving_units TEXT,
            calories_total INTEGER,
            calories_fat INTEGER,
            total_fat DECIMAL(10,2),
            saturated_fat DECIMAL(10,2),
            cholesterol INTEGER,
            sodium INTEGER,
            carbohydrates DECIMAL(10,2),
            fiber DECIMAL(10,2),
            protein DECIMAL(10,2),
            vitamin_a INTEGER,
            vitamin_c INTEGER,
            calcium INTEGER,
            iron INTEGER,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, manufacturer)
        )
    """)
    conn.commit()
    
    insert_sql = """
        INSERT INTO nutrition_data (
            name, manufacturer, serving_value, serving_units,
            calories_total, calories_fat, total_fat, saturated_fat,
            cholesterol, sodium, carbohydrates, fiber, protein,
            vitamin_a, vitamin_c, calcium, iron
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (name, manufacturer) 
        DO UPDATE SET
            serving_value = EXCLUDED.serving_value,
            serving_units = EXCLUDED.serving_units,
            calories_total = EXCLUDED.calories_total,
            calories_fat = EXCLUDED.calories_fat,
            total_fat = EXCLUDED.total_fat,
            saturated_fat = EXCLUDED.saturated_fat,
            cholesterol = EXCLUDED.cholesterol,
            sodium = EXCLUDED.sodium,
            carbohydrates = EXCLUDED.carbohydrates,
            fiber = EXCLUDED.fiber,
            protein = EXCLUDED.protein,
            vitamin_a = EXCLUDED.vitamin_a,
            vitamin_c = EXCLUDED.vitamin_c,
            calcium = EXCLUDED.calcium,
            iron = EXCLUDED.iron,
            loaded_at = CURRENT_TIMESTAMP
    """
    cursor.executemany(insert_sql, records)
    conn.commit()
    
    inserted_count = cursor.rowcount

    cursor.close()
    conn.close()
    
    return f"LOADED: {inserted_count} records"

with DAG(
    dag_id='xml_etl',
    schedule='@daily',
    start_date=datetime(2026, 1, 20),
    catchup=False,
    tags=['xml', 'nutrition', 'postgres', 'etl', 'full'],
    default_args={
        'retries': 1,
    }
) as dag:
    extract_task = PythonOperator(
        task_id='extract_and_parse_xml',
        python_callable=extract_and_parse_xml,
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )
    
    extract_task >> transform_task >> load_task