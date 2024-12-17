from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, from_json
import psycopg2

# Constants
TOPIC_REDDIT = "reddit-sentiments"
KAFKA_BOOTSTRAP_SERVERS = "course-kafka:9092"

POSTGRESQL_HOST = "postgres"
POSTGRESQL_PORT = "5432"
POSTGRESQL_DATABASE = "postgres"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_TABLE = "reddit_data"

S3_OUTPUT_PATH = "s3a://final-project-reddit/data/"
S3_CHECKPOINT_PATH = "s3a://final-project-reddit/checkpoint/"

# Define schema for Reddit data
schema = StructType([
    StructField("date", StringType(), True),
    StructField("stock", StringType(), True),
    StructField("average_sentiment", DoubleType(), True),
    StructField("comment_count", IntegerType(), True),
])


def write_to_postgres(df, batch_id):
    """
    Write DataFrame to PostgreSQL table.
    :param df: Spark DataFrame
    :param batch_id: Batch ID
    """
    try:
        print(f"Writing batch {batch_id} to PostgreSQL:")
        df.show()

        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_PORT,
            database=POSTGRESQL_DATABASE,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
        )
        cursor = connection.cursor()

        # Create table if it doesn't exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {POSTGRESQL_TABLE} (
                id SERIAL PRIMARY KEY,
                date TEXT,
                stock TEXT,
                average_sentiment DOUBLE PRECISION,
                comment_count INTEGER
            );
        """)

        # Insert data into PostgreSQL
        for row in df.collect():
            cursor.execute(f"""
                INSERT INTO {POSTGRESQL_TABLE} (date, stock, average_sentiment, comment_count)
                VALUES (%s, %s, %s, %s);
            """, (row.date, row.stock, row.average_sentiment, row.comment_count))

        # Commit changes and close connection
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Batch {batch_id} content:")
        df.show()
        print(f"Batch {batch_id} written to PostgreSQL successfully.")

    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")


if __name__ == "__main__":
    # Initialize Spark session
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("RedditKafkaToPostgres")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .config("spark.jars", "/opt/drivers/postgresql-42.3.6.jar")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    # Read data from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_REDDIT)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    # Parse the Kafka messages and apply schema
    parsed_stream = (
        raw_stream.select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # Write stream to PostgreSQL using foreachBatch
    postgres_query = (
        parsed_stream.writeStream.foreachBatch(write_to_postgres)
        .outputMode("append")
        .start()
    )

    # Write stream to S3
    s3_query = (
        parsed_stream.writeStream
        .format("parquet")
        .option("path", S3_OUTPUT_PATH)
        .option("checkpointLocation", S3_CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    # Print schema for debugging
    parsed_stream.printSchema()


    # Await termination for both queries
    postgres_query.awaitTermination()
    s3_query.awaitTermination()
