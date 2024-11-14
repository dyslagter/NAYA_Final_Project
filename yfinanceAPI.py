import yfinance as yf
import pandas as pd

# List of S&P 500 tickers
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Example stock list

# Fetch historical data for all tickers at once
try:
    df = yf.download(tickers, period="5d", group_by="ticker", auto_adjust=True)
except Exception as e:
    print(f"Error fetching data: {e}")
    exit(1)

data = []

# Iterate through the tickers and process the data
for ticker in tickers:
    try:
        history = df[ticker]

        # Ensure there are at least 2 days of data
        if len(history) < 2:
            print(f"Not enough data for {ticker}")
            continue

        # Calculate current price, previous close, change, and change percentage
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

    except KeyError:
        print(f"Data not available for {ticker}")
    except Exception as e:
        print(f"Error processing {ticker}: {e}")

# Create a DataFrame for the summary
result_df = pd.DataFrame(data)

# Display the DataFrame
print(result_df)

# Optionally, save to CSV
result_df.to_csv("stock_summary.csv", index=False)
print("Data saved to stock_summary.csv")
