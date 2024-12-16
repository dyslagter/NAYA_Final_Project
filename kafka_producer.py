import yfinance as yf
import json
import time
import logging
from kafka import KafkaProducer
import signal
import sys
from datetime import datetime, timedelta

# Logging configuration
logging.basicConfig(
    level=logging.INFO
)  # Configure logging to display info-level messages
logger = logging.getLogger(__name__)  # Create a logger instance

# Kafka configuration
KAFKA_BROKER = "course-kafka:9092"  # Address of the Kafka broker
TOPIC_NAME = "stock-data"  # Name of the Kafka topic to send stock data

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,  # Kafka broker connection
    value_serializer=lambda v: json.dumps(v).encode(
        "utf-8"
    ),  # Serialize data to JSON format
    retries=5,  # Number of retries for failed messages
    acks="all",  # Ensure message is fully committed
    request_timeout_ms=10000,  # Timeout for producer requests
)

# Parameter to control the month number
MONTH_NUMBER = 10  # Set the desired month number here (1-12)


# Load tickers from JSON file
def load_tickers(file_path):
    """
    Load stock tickers from a JSON file.
    :param file_path: Path to the JSON file containing ticker symbols.
    :return: List of stock tickers.
    """
    try:
        with open(file_path, "r") as file:  # Open the JSON file
            stock_data = json.load(file)  # Load the file content into a dictionary
        return list(stock_data.keys())  # Extract the keys (tickers) as a list
    except Exception as e:
        logger.error(
            f"Error loading tickers from JSON: {e}"
        )  # Log error if file loading fails
        return []


# Graceful shutdown
def handle_exit(signum, frame):
    """
    Gracefully close the Kafka producer and exit.
    :param signum: Signal number.
    :param frame: Stack frame.
    """
    logger.info("Shutting down...")  # Log shutdown message
    producer.close()  # Close Kafka producer
    sys.exit(0)  # Exit the program


# Register signal handlers for SIGINT and SIGTERM
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


def fetch_stock_data_for_month(tickers, month):
    """
    Fetch stock data day by day for a given month and send it to Kafka.
    :param tickers: List of stock tickers.
    :param month: The target month (1-12).
    """
    current_year = datetime.now().year  # Get the current year
    start_date = datetime(current_year, month, 1)  # Start of the specified month
    end_date = (start_date + timedelta(days=31)).replace(
        day=1
    )  # First day of the next month

    # Generate a list of dates for the month
    date_range = [
        start_date + timedelta(days=i) for i in range((end_date - start_date).days)
    ]

    # Dictionary to track the previous day's closing price for each ticker
    previous_day_prices = {ticker: None for ticker in tickers}

    for date in date_range:  # Iterate through each day in the month
        day_str = date.strftime("%Y-%m-%d")  # Format date as string (YYYY-MM-DD)
        logger.info(
            f"Fetching data for date: {day_str}"
        )  # Log current date being processed

        try:
            # Fetch daily stock data for the specified tickers
            df = yf.download(
                tickers,
                start=day_str,
                end=(date + timedelta(days=1)).strftime("%Y-%m-%d"),
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
            )
        except Exception as e:
            logger.error(
                f"Error fetching data for {day_str}: {e}"
            )  # Log errors in data fetching
            continue

        for ticker in tickers:  # Process each ticker's data
            try:
                # Select the appropriate stock history for the ticker
                history = df[ticker] if len(tickers) > 1 else df
                if history.empty:  # Skip if no data is available
                    logger.warning(f"No data for {ticker} on {day_str}")
                    continue

                # Get the closing price for the current day
                current_price = history["Close"].iloc[-1]
                # Use the previous day's price for calculating change, fallback to current price
                previous_close = (
                    previous_day_prices[ticker]
                    if previous_day_prices[ticker]
                    else current_price
                )
                change = current_price - previous_close  # Calculate price change
                change_percent = (
                    (change / previous_close) * 100 if previous_close else 0.0
                )  # Calculate percentage change

                # Update the previous day's price for the next iteration
                previous_day_prices[ticker] = current_price

                # Prepare stock data for Kafka
                stock_data = {
                    "Ticker": ticker,
                    "Current Price": round(current_price, 2),
                    "Change": round(change, 2),
                    "Change (%)": round(change_percent, 2),
                    "Date": day_str,
                }
                send_data(stock_data)  # Send the stock data to Kafka
            except Exception as e:
                logger.error(
                    f"Error processing {ticker} for {day_str}: {e}"
                )  # Log errors in processing


def send_data(stock_data):
    """
    Send stock data to the Kafka topic.
    :param stock_data: Dictionary containing stock information.
    """
    if stock_data:
        message = stock_data  # Prepare the message to send
        try:
            producer.send(TOPIC_NAME, value=message)  # Send data to Kafka topic
            producer.flush()  # Ensure data is flushed to Kafka
            logger.info(f"Sent data to Kafka: {message}")  # Log successful sending
        except Exception as e:
            logger.error(
                f"Error sending data to Kafka: {e}"
            )  # Log errors in Kafka transmission


if __name__ == "__main__":
    tickers = load_tickers("stocks_list.json")  # Load stock tickers from JSON file
    if tickers:  # Proceed if tickers were loaded successfully
        fetch_stock_data_for_month(
            tickers, MONTH_NUMBER
        )  # Fetch stock data for the specified month
    else:
        logger.error(
            "No tickers found in the JSON file."
        )  # Log error if no tickers are found
