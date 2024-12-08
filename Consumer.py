from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql.functions import col, from_json

# Constants
TOPIC_REDDIT = 'reddit-sentiments'
KAFKA_BOOTSTRAP_SERVERS = 'course-kafka:9092'
S3_OUTPUT_PATH = "s3a://final-project-reddit/data/"
S3_CHECKPOINT_PATH = "s3a://final-project-reddit/checkpoint/"

# Schema for the incoming data
def get_reddit_schema():
    return t.StructType([
        t.StructField("date", t.StringType(), True),
        t.StructField("stock", t.StringType(), True),
        t.StructField("average_sentiment", t.DoubleType(), True),
        t.StructField("comment_count", t.IntegerType(), True),
    ])

# Initialize Spark Session
def create_spark_session(app_name: str):
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .getOrCreate()
    )

# Read from Kafka
def read_from_kafka(spark, topic: str, bootstrap_servers: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
#        .option("failOnDataLoss","false")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    )

# Transform Kafka data into a DataFrame with schema
def parse_kafka_messages(kafka_df, schema):
    return (
        kafka_df
        .select(col("value").cast("string"))
        .select(from_json(col("value"), schema).alias("value"))
        .select("value.*")
    )

# Write the parsed data to S3
def write_to_s3(parsed_df, output_path: str, checkpoint_path: str):
    return (
        parsed_df.writeStream
        .format("json")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
    )

if __name__ == "__main__":
    # Initialize Spark session
    spark = create_spark_session("Reddit Consumer to S3")

    # Define schema
    reddit_schema = get_reddit_schema()

    # Read data from Kafka
    kafka_df = read_from_kafka(spark, TOPIC_REDDIT, KAFKA_BOOTSTRAP_SERVERS)

    # Transform the Kafka data
    stocks_df = parse_kafka_messages(kafka_df, reddit_schema)

    # Write transformed data to S3
    query = write_to_s3(stocks_df, S3_OUTPUT_PATH, S3_CHECKPOINT_PATH)

    # Print schema for verification
    stocks_df.printSchema()

    # Wait for the streaming to finish
    query.awaitTermination()
