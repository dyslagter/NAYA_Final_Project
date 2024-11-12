import json
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BROKER = "course-kafka:9092"
TOPIC_NAME = "stock-data"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="stock-consumers",
)

print("Listening for messages...")

for message in consumer:
    stock_data = message.value
    print(f"Received data: {stock_data}")
