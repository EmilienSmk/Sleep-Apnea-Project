from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def start_processing():
    spark = SparkSession.builder \
        .appName("SleepApneaProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    sensor_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("patient_id", StringType(), True),
        StructField("spo2", IntegerType(), True),
        StructField("heart_rate", IntegerType(), True)
    ])


    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor_vitals") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = raw_kafka_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), sensor_schema).alias("data")) \
        .select("data.*")


    aggregated_df = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds"), col("patient_id")) \
        .agg(
            avg("spo2").alias("spo2"),
            avg("heart_rate").alias("heart_rate")
        )


    kafka_output_df = aggregated_df \
        .select(
            col("patient_id").alias("key"),
            to_json(struct(
                col("window.end").alias("timestamp"), 
                col("patient_id"),
                col("spo2").cast("integer").alias("spo2"),
                col("heart_rate").cast("integer").alias("heart_rate")
            )).alias("value")
        )

    print("Spark Streaming Job Started. Pushing aggregated data to 'processed_vitals' topic...")
    

    query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "processed_vitals") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_processing()
