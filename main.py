import yfinance as yf
import pandas as pd

# List of S&P 500 tickers
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Example stock list

data = []

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
print(df)
