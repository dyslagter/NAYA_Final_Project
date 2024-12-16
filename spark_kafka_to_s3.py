from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, date_format

# Initialize Spark session
spark = (
    SparkSession.builder.master("local[*]")
    .appName("KafkaToS3")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# Define Kafka and S3 configurations
KAFKA_BROKER = "course-kafka:9092"
KAFKA_TOPIC = "stock-data"
S3_OUTPUT_PATH = "s3a://final-project-yahoo/stock_data/"

# Define schema for the incoming JSON data
schema = StructType(
    [
        StructField("Ticker", StringType(), True),
        StructField("Current Price", FloatType(), True),
        StructField("Change", FloatType(), True),
        StructField("Change (%)", FloatType(), True),
        StructField("Date", StringType(), True),
    ]
)

# Read data from Kafka topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the JSON data
value_df = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), schema).alias("data")
)
stock_df = value_df.select("data.*")

# Add partition columns for year and month
partitioned_df = stock_df.withColumn(
    "Year", date_format(col("Date"), "yyyy")
).withColumn("Month", date_format(col("Date"), "MM"))

# Write data to S3 in Parquet format, partitioned by year and month
query = (
    partitioned_df.writeStream.format("parquet")
    .option(
        "checkpointLocation", "/tmp/spark-checkpoint"
    )  # Ensure checkpointing for exactly-once semantics
    .option("path", S3_OUTPUT_PATH)  # Output path in S3
    .partitionBy("Year", "Month")  # Partition by year and month
    .outputMode("append")
    .start()
)

query.awaitTermination()
