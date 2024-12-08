import praw
from praw.models import MoreComments  # Add this import
import re
from collections import defaultdict
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta, timezone
import json
from confluent_kafka import Producer

# Constants
TOPIC_REDDIT = 'reddit-sentiments'
KAFKA_CONFIG = {
    'bootstrap.servers': 'course-kafka:9092',
    'client.id': 'producer_reddit_data',
}

STOCKS = {
    'AAPL': 'Apple',
    'META': 'Facebook',
    'GOOGL': 'Google',
    'NFLX': 'Netflix',
    'NVDA': 'Nvidia',
}


# Initialize Reddit API
def initialize_reddit():
    return praw.Reddit(
        client_id="8n0n0_sRlznj_5-74siNAw",
        client_secret="yfMviLoEGMO4x53SGdJUZPUKokxl3Q",
        user_agent="test1_de_course",
    )


# Generate regex patterns for stock symbols and company names
def generate_regex_patterns(stocks):
    keys = stocks.keys()
    values = stocks.values()
    stock_symbols_pattern = re.compile(r'\b(?:{})\b'.format("|".join(keys)))
    company_names_pattern = re.compile(r'\b(?:{})\b'.format("|".join(values)), re.IGNORECASE)
    return stock_symbols_pattern, company_names_pattern


# Fetch posts from the last 7 days
def fetch_posts(subreddit_name, reddit_instance):
    subreddit = reddit_instance.subreddit(subreddit_name)
    seven_days_ago = datetime.now() - timedelta(days=7)
    return [
        post for post in subreddit.new(limit=None)
        if datetime.fromtimestamp(post.created_utc) >= seven_days_ago
    ]


# Process comments and extract stock data, aggregating by day
def process_comments(posts, stock_symbols_pattern, company_names_pattern, analyzer, stocks):
    daily_data = defaultdict(lambda: defaultdict(lambda: {
        "sentiments": [],
        "comment_count": 0,
    }))

    for post in posts:
        post.comments.replace_more(limit=None)
        for comment in post.comments.list():
            if isinstance(comment, MoreComments):
                continue

            comment_date = datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).date()
            symbols_mentioned = stock_symbols_pattern.findall(comment.body)
            companies_mentioned = company_names_pattern.findall(comment.body)

            stocks_mentioned_in_comment = set()
            for symbol in symbols_mentioned:
                analyze_comment(
                    symbol, comment, analyzer, daily_data[comment_date], stocks_mentioned_in_comment
                )

            for company in companies_mentioned:
                stock_name = next((key for key, value in stocks.items() if value.lower() == company.lower()), None)
                if stock_name:
                    analyze_comment(
                        stock_name, comment, analyzer, daily_data[comment_date], stocks_mentioned_in_comment
                    )

            for stock in stocks_mentioned_in_comment:
                daily_data[comment_date][stock]["comment_count"] += 1

    return daily_data


# Analyze individual comments for sentiment, aggregating by day
def analyze_comment(symbol, comment, analyzer, daily_data, mentioned_set):
    if symbol not in mentioned_set:
        mentioned_set.add(symbol)

    sentiment_scores = analyzer.polarity_scores(comment.body)
    daily_data[symbol]["sentiments"].append(sentiment_scores['compound'])


# Aggregate results for final output per day
def aggregate_results(daily_data):
    final_data = []

    for date, stocks_data in daily_data.items():
        for stock, data in stocks_data.items():
            final_data.append({
                "date": date.strftime("%Y-%m-%d"),  # Convert date to string
                "stock": stock,
                "average_sentiment": sum(data["sentiments"]) / len(data["sentiments"]) if data["sentiments"] else 0,
                "comment_count": data["comment_count"],
            })

    return final_data


# Send data to Kafka
def send_data_to_kafka(producer, data, topic):
    producer.produce(topic, key=data['stock'], value=json.dumps(data))
    producer.flush()


# Main script
def main():
    reddit = initialize_reddit()  # Initialize the Reddit API client
    stock_symbols_pattern, company_names_pattern = generate_regex_patterns(
        STOCKS)  # Create regex patterns for symbols and companies
    posts = fetch_posts("stocks", reddit)  # Fetch the Reddit posts
    analyzer = SentimentIntensityAnalyzer()  # Initialize sentiment analyzer

    # Process comments and aggregate daily data
    daily_data = process_comments(posts, stock_symbols_pattern, company_names_pattern, analyzer, STOCKS)

    # Aggregate the final results for output
    final_results = aggregate_results(daily_data)

    # Prepare data for Kafka
    producer = Producer(KAFKA_CONFIG)
    for result in final_results:
        data = {
            'stock': result['stock'],
            'date': result['date'],
            'average_sentiment': result['average_sentiment'],
            'comment_count': result['comment_count'],
        }
        send_data_to_kafka(producer, data, TOPIC_REDDIT)


if __name__ == "__main__":
    main()