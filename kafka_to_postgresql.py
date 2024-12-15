from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json
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
spark = SparkSession.builder.appName("KafkaToPostgres").getOrCreate()

# Define schema for stock data
schema = StructType(
    [
        StructField("Ticker", StringType(), True),
        StructField("Current Price", FloatType(), True),
        StructField("Change", FloatType(), True),
        StructField("Change (%)", FloatType(), True),
    ]
)


def write_to_postgres(df, batch_id):
    try:
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
                ticker VARCHAR(10) PRIMARY KEY,
                current_price FLOAT NOT NULL,
                change FLOAT NOT NULL,
                change_percent FLOAT NOT NULL
            );
        """
        )

        # Insert data into PostgreSQL
        for row in df.collect():
            cursor.execute(
                f"""
                INSERT INTO {POSTGRESQL_TABLE} (ticker, current_price, change, change_percent)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE 
                SET current_price = EXCLUDED.current_price,
                    change = EXCLUDED.change,
                    change_percent = EXCLUDED.change_percent;
            """,
                (row.Ticker, row["Current Price"], row.Change, row["Change (%)"]),
            )

        # Commit changes and close connection
        connection.commit()
        cursor.close()
        connection.close()
        print("Batch written to PostgreSQL.")

    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")


# Read data from Kafka topic
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .load()
)

# Extract value from Kafka message and parse JSON
parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) as json")
    .select(from_json("json", schema).alias("data"))
    .select("data.*")
)

# Write stream to PostgreSQL using foreachBatch
query = parsed_stream.writeStream.foreachBatch(write_to_postgres).start()

query.awaitTermination()
