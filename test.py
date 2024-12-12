from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import datetime

# Configuration
KAFKA_BROKER = "course-kafka:9092"
TOPIC_NAME = "stock-data"
S3_BUCKET = "s3a://your-bucket-name"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_ENDPOINT = "http://minio:9000"

# # Initialize Spark Session
# spark = (
#     SparkSession.builder.appName("Kafka-to-S3")
#     .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
#     .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
#     .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#     .getOrCreate()
# )
spark = (
    SparkSession.builder.appName("Kafka-to-S3")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
# Define schema for incoming Kafka data
schema = StructType(
    [
        StructField(
            "stocks",
            StructType(
                [
                    StructField("Ticker", StringType(), True),
                    StructField("Current Price", DoubleType(), True),
                    StructField("Change", DoubleType(), True),
                    StructField("Change (%)", DoubleType(), True),
                ]
            ),
            True,
        )
    ]
)

# Read Kafka Stream
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

# Decode and parse Kafka messages
df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .selectExpr("data.stocks.*")
)

# Add a date column based on the current processing date
df = df.withColumn("date", current_date())


# Output function to save data as Parquet
def write_to_s3(df, epoch_id):
    date = datetime.date.today().isoformat()
    output_path = f"{S3_BUCKET}/{date}/stocks.parquet"
    df.write.mode("append").parquet(output_path)
    print(f"Written data to {output_path}")


# Write stream to S3
query = df.writeStream.foreachBatch(write_to_s3).outputMode("append").start()

# Wait for termination
query.awaitTermination()
