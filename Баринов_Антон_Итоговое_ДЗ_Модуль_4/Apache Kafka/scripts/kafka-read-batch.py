#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object



def main():
    spark = SparkSession.builder \
        .appName('kafka-json-flatten-final') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .getOrCreate()
    
    df_raw = spark.read.format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-00c16j6q49b7pt7o.mdb.yandexcloud.net:9091') \
        .option('subscribe', 'dataproc-kafka-topic') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config',
                'org.apache.kafka.common.security.scram.ScramLoginModule required '
                'username=user1 '
                'password=password1 '
                ';') \
        .option('startingOffsets', 'earliest') \
        .load() \
        .selectExpr("CAST(value AS STRING) as raw_message") \
        .where(col("raw_message").isNotNull())
    
    df_flat = df_raw.select(
        get_json_object(col("raw_message"), "$.msg").alias("inner_json")
    ).select(
        get_json_object(col("inner_json"), "$.application_id").alias("application_id"),
        get_json_object(col("inner_json"), "$.customer.customer_id").alias("customer_id"),
        get_json_object(col("inner_json"), "$.customer.region").alias("region"),
        get_json_object(col("inner_json"), "$.loan.amount").alias("loan_amount"),
        get_json_object(col("inner_json"), "$.loan.term_months").alias("loan_term_months"),
        get_json_object(col("inner_json"), "$.scoring.score").alias("score"),
        get_json_object(col("inner_json"), "$.scoring.risk_level").alias("risk_level"),
        get_json_object(col("inner_json"), "$.documents").alias("documents_json"),
        get_json_object(col("inner_json"), "$.decision_status").alias("decision_status"),
        get_json_object(col("inner_json"), "$.submitted_at").alias("submitted_at")
    )
    
    df_flat.coalesce(1).write \
        .mode('overwrite') \
        .format('csv') \
        .option('header', 'true') \
        .save('s3a://dataproc-bucket/processed_data_batch')
    
    print('CSV saved')
    spark.stop()

if __name__ == '__main__':
    main()