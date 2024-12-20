from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, current_date
import psycopg2

# PostgreSQL configuration
POSTGRESQL_HOST = "postgres"
POSTGRESQL_PORT = "5432"
POSTGRESQL_DATABASE = "postgres"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_TABLE = "stock_data"

# Kafka configuration
KAFKA_BROKER = "course-kafka:9092"
TOPIC_NAME = "stock-data"

# Initialize Spark session
spark = (
    SparkSession.builder.master("local[*]")
    .appName("KafkaToPostgres")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .config("spark.jars", "/opt/drivers/postgresql-42.3.6.jar")
    .getOrCreate()
)

# Define schema for stock data
schema = StructType(
    [
        StructField("Ticker", StringType(), True),
        StructField("Current Price", FloatType(), True),
        StructField("Change", FloatType(), True),
        StructField("Change (%)", FloatType(), True),
        StructField("Date", StringType(), True),  # Add Date column
    ]
)


def write_to_postgres(df, batch_id):
    """
    Write DataFrame to PostgreSQL table.
    :param df: Spark DataFrame
    :param batch_id: Batch ID
    """
    try:
        # Debugging: Print DataFrame schema and data
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
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {POSTGRESQL_TABLE} (
                ticker VARCHAR(10),
                current_price FLOAT NOT NULL,
                change FLOAT NOT NULL,
                change_percent FLOAT NOT NULL,
                date DATE NOT NULL
            );
        """
        )

        # Insert data into PostgreSQL
        for row in df.collect():
            cursor.execute(
                f"""
                INSERT INTO {POSTGRESQL_TABLE} (ticker, current_price, change, change_percent, date)
                VALUES (%s, %s, %s, %s, %s)
            """,
                (
                    row.Ticker,
                    row["Current Price"],
                    row.Change,
                    row["Change (%)"],
                    row.Date,
                ),
            )

        # Commit changes and close connection
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Batch {batch_id} written to PostgreSQL successfully.")

    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")


# Read data from Kafka topic
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")  # Read all data from Kafka topic
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
)

# Extract value from Kafka message, parse JSON, and add current date
parsed_stream = (
    raw_stream.select(col("value").cast("string"))
    .select(from_json(col("value"), schema).alias("value"))
    .select("value.*")
    .withColumn("Date", col("Date").cast("date"))  # Ensure Date column is of type DATE
)

# Write stream to PostgreSQL using foreachBatch
query = (
    parsed_stream.writeStream.foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()
