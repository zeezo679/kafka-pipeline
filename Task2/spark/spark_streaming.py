import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType
)

#constants
HDFS_BASE_URL = "hdfs://localhost:9000" #from docker-compose
DATA_OUTPUT_PATH = f"{HDFS_BASE_URL}/user/hive/warehouse/ecommerce_dw.db/streaming_orders"
CHECKPOINT_PATH = f"{HDFS_BASE_URL}/user/spark/checkpoints/streaming_orders"

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("KafkaToHdfsStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .getOrCreate()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    order_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DecimalType(), True),
        StructField("order_time", StringType(), True)  
    ])

    kafka_df = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("subscribe", "topic1_logs") \
                    .option("startingOffsets", "earliest") \
                    .option("failOnDataLoss", "false") \
                    .load()

    parsed_kafka_df = (
        kafka_df
            .selectExpr("CAST(value as string) as json_payload")
            .select(from_json(col("json_payload"), order_schema).alias("data"))
            .select("data.*")    
            .withColumn("order_time", col("order_time").cast(TimestampType())) #casting to timestamp type
    )


    enriched_df = parsed_kafka_df.withColumn("total_amount", (col("quantity") * col("price")).cast(DecimalType()))

    query = enriched_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", DATA_OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()

    print(f"Streaming write started to HDFS path: {DATA_OUTPUT_PATH}")
    query.awaitTermination()


main()