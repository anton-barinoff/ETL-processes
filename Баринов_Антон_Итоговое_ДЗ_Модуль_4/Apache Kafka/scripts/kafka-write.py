#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col



def main():
    spark = SparkSession.builder \
        .appName('json-to-kafka') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .getOrCreate()

    df = spark.read.text('s3a://dataproc-bucket/messages.json')
    df = df.select(col('value').alias('value'))
    
    print(f"Read {df.count()} messages")
    print("Sending to Kafka")
    
    df.write.format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-00c16j6q49b7pt7o.mdb.yandexcloud.net:9091') \
        .option('topic', 'dataproc-kafka-topic') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('kafka.sasl.jaas.config',
                'org.apache.kafka.common.security.scram.ScramLoginModule required '
                'username=user1 '
                'password=password1 '
                ';') \
        .option('kafka.batch.size', 16384) \
        .option('kafka.linger.ms', 100) \
        .save()
    
    print("Data sent to Kafka")
    spark.stop()

if __name__ == '__main__':
    main()