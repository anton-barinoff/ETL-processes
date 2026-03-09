# ETL-проект: Репликация данных из MongoDB в PostgreSQL с использованием Apache Airflow

## Описание проекта
Проект реализует ETL-пайплайн для репликации данных из MongoDB в PostgreSQL с последующим созданием аналитических витрин. 
Данные имитируют работу онлайн-кинотеатра (просмотры фильмов, платежи, оценки, поисковые запросы).

## Архитектура
MongoDB (источник) → Airflow (ETL) → PostgreSQL (хранилище) → Аналитические витрины

## Модель данных

### MongoDB (source)
- **movie_views** — просмотры фильмов (100 документов);
- **user_payments** — платежи пользователей (60 документов);
- **content_ratings** — оценки фильмов (80 документов);
- **search_queries** — поисковые запросы (50 документов).

### PostgreSQL (staging)
Те же структуры, но с реляционной схемой и дополнительными трансформациями:
- Разворачивание вложенных структур (device → device_type, device_os);
- Преобразование массивов в JSONB;
- Добавление технических полей (loaded_at, source).

## Пайплайны репликации

### 1. Основной DAG: 'mongo_to_postgres'
```
# Расположение: dags/mongo_to_postgres.py
# Описание: Извлекает все 4 коллекции из MongoDB, трансформирует и загружает в PostgreSQL.

# Этапы:
	- Extract - Подключение к MongoDB, исключение поля _id, передача в XCom.
	- Transform & Load - Удаление дублей, приведение типов, разворачивание JSON, добавление технических полей & Создание таблиц, удаление старых данных за день, вставка новых.
	- Validate - Подсчет строк в каждой таблице, вывод статистики.
	
# Пайплайн:
	extract >> transform_load >> validate
```

### 2. Альтернативный DAG с параллельными задачами: 'pipeline_mongo_postgres'
```
# Расположение: dags/pipeline_mongo_postgres.py
# Описание: Извлекает все 4 коллекции из MongoDB, параллельно трансформирует и загружает их в PostgreSQL.

# Этапы:
	- Extract - Чтение данных из MongoDB - Подключение к MongoDB, исключение поля _id, передача в XCom.
	- Transform - Очистка и обогащение данных, выполняется параллельно для каждого источника.	
	- Load - Запись в PostgreSQL, выполняется параллельно для каждой таблицы.
	- Validate - Проверка результатов - Подсчет строк в каждой таблице, вывод статистики.

# Пайплайн:
	extract >> [transform_movies, transform_payments, transform_ratings, transform_searches]

		transform_movies >> load_movies
		transform_payments >> load_payments
		transform_ratings >> load_ratings
		transform_searches >> load_searches
               
		[load_movies, load_payments, load_ratings, load_searches] >> validate
```


### 3. DAG аналитической витрины: 'mart_content_performance'
```
# Расположение: dags/mart_content_performance.py
# Описание: Создает витрину для аналитики по фильмам.

# Витрина mart_content_performance содержит:
	- movie_id — идентификатор фильма;
	- movie_title — название фильма;
	- genre — жанр;
	- total_views — общее кол-во просмотров;
	- unique_viewers — кол-во уникальных зрителей;
	- avg_watch_duration — среднее время просмотра;
	- avg_rating — средняя оценка;
	- total_ratings — кол-во оценок;
	- search_appearances - кол-во поисковых запросов;
	- search_to_view_ratio - коэффициэнт просмотров к поисковым запросам.

# Пайплайн:
    check_data >> create_mart
```

### 4. DAG аналитической витрины: 'mart_payment_analysis'
```
# Расположение: dags/mart_payment_analysis.py
# Описание: Создает витрину для анализа платежей пользователей.

# Витрина mart_payment_analysis содержит:
	- user_id — идентификатор пользователя;
	- subscription_plan — тип подписки;
	- total_payments — кол-во платежей;
	- total_revenue — общая сумма;
	- first_payment / last_payment — даты первого и последнего платежа;
	- avg_payment_amount — средний чек;
	- preferred_payment_method — предпочтительный способ оплаты;
	- successful_payments — кол-во успешных платежей.
	
# Пайплайн:
    check_data >> create_mart
```

### 5. DAG аналитической витрины: 'mart_user_activity'
```
# Расположение: dags/mart_user_activity.py
# Описание: Создает витрину для анализа поведения пользователей.

# Витрина mart_user_activity содержит:
	- user_id — идентификатор пользователя;
	- total_views — общее кол-во просмотров;
	- total_watch_time_minutes — общее кол-во минут просмотра;
	- avg_watch_duration — среднее время просмотра;
	- unique_movies_watched — кол-во просмотренных уникальных фильмов;
	- favorite_genre — предпочитаемый жанр;
	- completion_rate — % доля завершенных просмотров;
	- last_activity_date — дата последней активности.
	
# Пайплайн:
    check_data >> create_mart
```