from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaToS3").getOrCreate()

# Define Kafka and S3 configurations
kafka_broker = "course-kafka:9092"
kafka_topic = "stock-data"
s3_output_path = "s3a://kafka-data/stock_data/"

# Set MinIO/S3 credentials
spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

# Define schema for the incoming JSON data
schema = StructType(
    [
        StructField("Ticker", StringType(), True),
        StructField("Current Price", FloatType(), True),
        StructField("Change", FloatType(), True),
        StructField("Change (%)", FloatType(), True),
    ]
)

# Read data from Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the JSON data
value_df = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), schema).alias("data")
)
stock_df = value_df.select("data.*")

# Write data to S3 in Parquet format
query = (
    stock_df.writeStream.format("parquet")
    .option("checkpointLocation", "/tmp/spark-checkpoint")
    .option("path", s3_output_path)
    .outputMode("append")
    .start()
)

query.awaitTermination()
