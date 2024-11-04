from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
import yfinance as yf
import pandas as pd
import json

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 31),
    'retries': 1,
}

tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Example stock list
data = []

def pull_from_api(**kwargs):
    tickers = kwargs['tickers']  # Access the tickers from kwargs
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        history = stock.history(period="5d")  # Fetch last five days of history

        # Check if history has at least 2 rows to calculate previous close
        if len(history) < 2:
            print(f"Not enough data for {ticker}")
            continue

        # Get the last two days' closing prices
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

    # Create a DataFrame for easier visualization
    df = pd.DataFrame(data)

    # Convert DataFrame to JSON and push to XCom
    df_json = df.to_json()  # Converts to JSON string
    print(df_json)
    kwargs['ti'].xcom_push(key='stockAPI_data', value=df_json)

def pull_from_xcom_push_to_topic(**kwargs):
    df_json = kwargs['ti'].xcom_pull(task_ids='task1', key='stockAPI_data')
    df = pd.read_json(df_json)

    # Convert DataFrame to JSON for Kafka
    df_records = df.to_dict(orient='records')
    print(df_records)

    # Create Producer instance
    producer = Producer({'bootstrap.servers': '192.168.1.119:9092'})

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    for record in df_records:
        producer.produce('stock_api', value=json.dumps(record), callback=delivery_report)
    producer.flush()


with DAG(
    'FromStockApiToKafka',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=pull_from_api,
        op_kwargs={'tickers': tickers},  # Pass the tickers as an argument
        provide_context=True,
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=pull_from_xcom_push_to_topic,
        provide_context=True,
    )

    task1 >> task2
