import yfinance as yf
import pandas as pd
import json
import time
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "course-kafka:9092"
TOPIC_NAME = "stock-data"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# List of tickers
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


def fetch_stock_data():
    data = []

    # Fetch historical data for all tickers
    try:
        df = yf.download(tickers, period="5d", group_by="ticker", auto_adjust=True)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

    # Process each ticker's data
    for ticker in tickers:
        try:
            history = df[ticker]
            if len(history) < 2:
                print(f"Not enough data for {ticker}")
                continue

            current_price = history["Close"].iloc[-1]
            previous_close = history["Close"].iloc[-2]
            change = current_price - previous_close
            change_percent = (change / previous_close) * 100

            data.append(
                {
                    "Ticker": ticker,
                    "Current Price": round(current_price, 2),
                    "Change": round(change, 2),
                    "Change (%)": round(change_percent, 2),
                }
            )

        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    return data


def send_data():
    while True:
        stock_data = fetch_stock_data()
        if stock_data:
            message = {"stocks": stock_data}
            producer.send(TOPIC_NAME, value=message)
            print(f"Sent data to Kafka: {message}")
        time.sleep(5)


if __name__ == "__main__":
    send_data()
