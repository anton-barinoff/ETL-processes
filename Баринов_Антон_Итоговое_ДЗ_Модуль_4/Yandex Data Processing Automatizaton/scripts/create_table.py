from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys



input_path = sys.argv[1]
output_path = sys.argv[2] 

spark = SparkSession.builder \
    .appName("ProcessCreditApplications") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print(f"Rows loaded: {df.count()}")

df = df \
    .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("approved_amount", F.col("approved_amount").cast("int")) \
    .withColumn("requested_amount", F.col("requested_amount").cast("int")) \
    .withColumn("credit_score", F.col("credit_score").cast("int")) \
    .withColumn("employee_review_flag",
                F.when(F.col("employee_review_flag") == "true", True).otherwise(False)) \
    .withColumn("amount_utilization",
                F.when(F.col("approved_amount") > 0,
                       F.round(F.col("approved_amount") / F.col("requested_amount"), 2))
                .otherwise(0.0)) \
    .withColumn("approval_flag",
                F.when(F.col("decision_status") == "approved", 1).otherwise(0))

result = df.groupBy("region_code", "product_type", "risk_level") \
    .agg(
        F.count("*").alias("total_applications"),
        F.sum("approved_amount").alias("total_approved_amount"),
        F.avg("credit_score").cast("int").alias("avg_credit_score"),
        F.round(F.avg("processing_time_sec"), 1).alias("avg_processing_time"),
        F.round(F.avg("amount_utilization"), 3).alias("avg_utilization"),
        F.round(F.sum("approval_flag") / F.count("*"), 3).alias("approval_rate")
    ) \
    .orderBy("region_code", "product_type", "risk_level")

result.show(20, False)

result.write.mode("overwrite").parquet(output_path)

print(f"Done\n  Output: {output_path}")
spark.stop()