from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import types as t
import pyspark.sql.functions as f
from pyspark.sql.types import *


TOPIC_REDDIT = 'reddit-sentiments'


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('reddit_consumer_to_s3') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()
    
    
# socketDF = spark \
# .readStream \
# .format("kafka") \
# .option("kafka.bootstrap.servers", "course-kafka:9092") \
# .option("Subscribe", "my_trip")\
# .load()\
# .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

# Create a streaming DataFrame from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'course-kafka:9092') \
    .option("subscribe", TOPIC_REDDIT) \
    .load() \
    .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

schema = t.StructType() \
    .add("comment_count", t.DecimalType())\
    .add("total_sentiment_avg", t.DecimalType()) \
    .add("stock", t.StringType())\
    .add("date", t.StringType()) \
    
#==============================================================================================
#==========================change json to dataframe with schema==============================#
stocksDF = kafka_df.select(f.col("value").cast("string")).select(f.from_json(f.col("value"), schema).alias("value")).select("value.*")


# stocksDF.write.parquet('s3a://FinalProjectReddit/data/', mode='overwrite')

# # Print the streaming data
# query = kafka_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


query = (
    kafka_df.writeStream
    .format("json")
    .option("path", "s3a://final-project-reddit/data/")
    .option("checkpointLocation", "s3a://final-project-reddit/checkpoint/")
    .outputMode("append")
    .start()
)



print(stocksDF)

print(query)

# Wait for termination
query.awaitTermination()
