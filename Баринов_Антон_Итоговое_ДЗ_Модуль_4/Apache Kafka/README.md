## Результаты выполнения Задания 3. Работа с топиками Apache Kafka® с помощью PySparkзаданий в Yandex Data Processing

### 1. Настройка инфраструктуры
- Создана облачная сеть `dataproc-network`;
- Создана подсеть `dataproc-subnet-b` в зоне доступности `ru-central1-b`;
- Настроен NAT-шлюз для подсети `dataproc-subnet-b`;
- Создана и настроена группа безопасности `dataproc-security-group` в сети `dataproc-network`;
- Создан сервисный аккаунт `dataproc-sa` с ролями: `storage.viewer`, `storage.uploader`, `storage.editor`, `dataproc.agent`, `dataproc.user`, `dataproc.editor`, `editor`;
- Создан бакет `dataproc-bucket`;
- Сервисному аккаунту `dataproc-sa` предоставлено разрешение `FULL_CONTROL` на бакет `dataproc-bucket`;
- Создан кластер Yandex Data Processing:
  - Имя: `dataproc-cluster`
  - Версия: `2.1`
  - Сервисы: `HDFS`, `LIVY`, `SPARK`, `TEZ`, `YARN`
  - Подкластеры: мастер, data, compute
- Создан кластер Managed Service for Apache Kafka®:
  - Имя: `dataproc-kafka`
  - Версия: `4.0.2`
  - Зона доступности: `ru-central1-b`
- Создан топик Apache Kafka®:
  - Имя: `dataproc-kafka-topic`
  - Количество разделов: `1`
  - Фактор репликации: `1`
- Создан пользователь Apache Kafka®:  
  - Имя: `user1`
  - Права: `ACCESS_ROLE_CONSUMER`, `ACCESS_ROLE_PRODUCER`, `ACCESS_ROLE_ADMIN` на все топики

### 2. Генерация тестовых данных

- Разработан скрипт `generate_json.py` для генерации JSON-сообщений в формате, соответствующем заданию.
- Сгенерировано `60000` сообщений (размер файла ~20 МБ).
- Пример сгенерированного сообщения:
  ```json
  {
    "application_id": "loan_205253",
    "customer": {
      "customer_id": "cust_1919",
      "region": "DE-BW"
    },
    "loan": {
      "amount": 10106,
      "term_months": 48
    },
    "scoring": {
      "score": 814,
      "risk_level": "low"
    },
    "documents": [
      {"type": "driver_license", "status": "rejected"},
      {"type": "utility_bill", "status": "pending"}
    ],
    "decision_status": "rejected",
    "submitted_at": "2026-06-07T01:48:05Z"
  }
  ```
### 3. Разработанные PySpark-задания
3.1. Запись данных в Kafka (`kafka-write.py`):
Скрипт читает JSON-файл из бакета и отправляет каждую строку как отдельное сообщение в топик Kafka.

3.2. Пакетное чтение из Kafka (`kafka-read-batch.py`):
Скрипт читает все сообщения из Kafka, парсит JSON и сохраняет в плоский CSV.
	Результат пакетной обработки:
	```text
	application_id,customer_id,region,loan_amount,loan_term_months,score,risk_level,documents_json,decision_status,submitted_at
	loan_274518,cust_848,DE-RP,13594,36,483,high,"[{\"type\":\"utility_bill\",\"status\":\"pending\"}]",pending,2026-06-13T16:24:17Z
	loan_303855,cust_818,DE-SN,23711,48,775,medium,"[{\"type\":\"passport\",\"status\":\"verified\"},{\"type\":\"passport\",\"status\":\"rejected\"}]",approved,2026-06-08T16:24:17Z
	...
	```
	
3.3. Потоковое чтение из Kafka (kafka-read-stream.py):
Скрипт реализует потоковую обработку (structured streaming) с триггером 10 секунд.

### 4. Возникшие проблемы и их решение
- Двойное JSON-кодирование в Kafka ({"msg":"{\"application_id\":...}"})	- использован get_json_object(col('raw_message'), '$.msg') для извлечения внутреннего JSON;
- Отсутствие логов df.show() в Data Proc - использован collect() и запись диагностических данных в бакет для отладки.

### 5. Результаты
- Инфраструктура полностью развернута и настроена;
- Сгенерирован тестовый набор данных объемом более 20 МБ;
- Реализована запись данных в Kafka через PySpark;
- Реализована пакетная обработка данных из Kafka с сохранением в CSV;
- Реализована потоковая обработка данных из Kafka;
- JSON-данные успешно преобразованы в плоский табличный вид;
- Итоговые CSV-файлы сохранены в Object Storage.