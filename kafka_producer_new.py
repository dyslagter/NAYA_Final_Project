import yfinance as yf
import json
import time
import logging
from kafka import KafkaProducer
import signal
import sys

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = "course-kafka:9092"
TOPIC_NAME = "stock-data"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all",
    request_timeout_ms=10000,
)

# List of tickers
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


# Graceful shutdown
def handle_exit(signum, frame):
    logger.info("Shutting down...")
    producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


def fetch_stock_data():
    data = []
    try:
        df = yf.download(
            tickers, period="1d", interval="1m", group_by="ticker", auto_adjust=True
        )
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return []

    for ticker in tickers:
        try:
            history = df[ticker]
            if len(history) < 2:
                logger.warning(f"Not enough data for {ticker}")
                continue

            current_price = history["Close"].iloc[-1]
            previous_close = history["Close"].iloc[-2]
            change = current_price - previous_close
            change_percent = (change / previous_close) * 100

            # data.append(
            #     {
            #         "Ticker": ticker,
            #         "Current Price": round(current_price, 2),
            #         "Change": round(change, 2),
            #         "Change (%)": round(change_percent, 2),
            #     }
            # )
            stock_data = {
                    "Ticker": ticker,
                    "Current Price": round(current_price, 2),
                    "Change": round(change, 2),
                    "Change (%)": round(change_percent, 2),
                }
            send_data(stock_data)
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")

    return data


def send_data(stock_data):
    # while True:
        # stock_data = fetch_stock_data()
        if stock_data:
            message = stock_data
            try:
                producer.send(TOPIC_NAME, value=message)
                producer.flush()
                logger.info(f"Sent data to Kafka: {message}")
            except Exception as e:
                logger.error(f"Error sending data to Kafka: {e}")
        # time.sleep(5)


if __name__ == "__main__":
    # send_data()
    fetch_stock_data()
