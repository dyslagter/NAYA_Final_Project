import praw
import re
from collections import defaultdict
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import datetime
import json
import csv 

# Set up Reddit API credentials
reddit = praw.Reddit(
    client_id="8n0n0_sRlznj_5-74siNAw",  # Replace with your Reddit API client_id
    client_secret="yfMviLoEGMO4x53SGdJUZPUKokxl3Q",  # Replace with your Reddit API client_secret
    user_agent="test1_de_course",  # Describe your application (e.g., 'my_app_v1')
)

# Define the stock symbols of interest
stocks = {
    'AAPL': 'Apple',
    'META': 'Facebook',  # Meta's old ticker was FB
    'GOOGL': 'Google',
    'NFLX': 'Netflix',
    'NVDA': 'Nvidia'
}

# Initialize a dictionary to count stock mentions and sentiment
stock_mentions = defaultdict(int)
stock_sentiments = defaultdict(list)
stock_comments = defaultdict(list)
stock_comment_count = defaultdict(int)
stock_comment_timestamps = defaultdict(lambda: [float('inf'), float('-inf')])  # [min_timestamp, max_timestamp]


# Extract all keys from the JSON data
keys = re.findall(r'"([^"]+)":', json.dumps(stocks))

print(keys)

def extract_values(obj):
    values = []
    if isinstance(obj, dict):
        for value in obj.values():
            values.extend(extract_values(value))
    elif isinstance(obj, list):
        for item in obj:
            values.extend(extract_values(item))
    else:
        values.append(obj)
    return values


values = extract_values(json.loads(json.dumps(stocks)))
print(values)


# Regex patterns for detecting stock symbols and company names
# stock_symbols_pattern = re.compile(r'\b(?:AAPL|META|GOOGL|NFLX|NVDA)\b')  # Add more symbols as needed

# Create a regular expression pattern with all keys
stock_symbols_pattern =  re.compile(r'\b(?:{})\b'.format("|".join(keys)))

# company_names_pattern = re.compile(r'\b(?:Apple|Facebook|Google|Netflix|Nvidia)\b', re.IGNORECASE)
company_names_pattern = re.compile(r'\b(?:{})\b'.format("|".join(values)), re.IGNORECASE)


# Initialize the sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Choose a subreddit
subreddit_name = "stocks"  # You can change this to any subreddit
subreddit = reddit.subreddit(subreddit_name)

# Fetch top posts (limit to top 5 for example)
# top_posts = subreddit.hot(limit=20)
# top_posts = subreddit.top(time_filter="day", limit=None)
today = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())

today_submissions = subreddit.new(limit=None)
top_posts = [submission for submission in today_submissions
               if datetime.datetime.fromtimestamp(submission.created_utc) >= today]

# Fetch the 5 most recent posts
for post in top_posts:
    # post_date = datetime.datetime.fromtimestamp(post.created_utc).date()
    # if post_date == today:
    post.comments.replace_more(limit=None)  # Replace 'load more comments' with actual comments

    # with open('C:\\Users\\Niv Junowicz\\Desktop\\data.csv', mode='a', newline='') as file:
    #     writer = csv.writer(file)

    #     # Write each row of the list to the CSV file
    #     for row in post.comments.list():
    #         writer.writerow([row.body.encode('utf-8')])
            
        
    # Loop through the comments in the post
    for comment in post.comments.list():
        
        if isinstance(comment, praw.models.MoreComments):
            continue
        
        symbols_mentioned = stock_symbols_pattern.findall(comment.body)
        # Check for company names
        companies_mentioned = company_names_pattern.findall(comment.body)

        stocks_mentioned_in_comment = set()

        # Count mentions and perform sentiment analysis for symbols
        for symbol in symbols_mentioned:
            stock_mentions[symbol] += 1  # Increment using the symbol directly
            stocks_mentioned_in_comment.add(symbol)

            sentiment_scores = analyzer.polarity_scores(comment.body)
            stock_sentiments[symbol].append(sentiment_scores['compound'])
            stock_comments[symbol].append(comment.body)

            # Update timestamps
            stock_comment_timestamps[symbol][0] = min(stock_comment_timestamps[symbol][0],
                                                      comment.created_utc)  # Min timestamp
            stock_comment_timestamps[symbol][1] = max(stock_comment_timestamps[symbol][1],
                                                      comment.created_utc)  # Max timestamp

        # Count mentions for company names
        for company in companies_mentioned:
            stock_name = next((key for key, value in stocks.items() if value.lower() == company.lower()), None)
            if stock_name:
                stock_mentions[stock_name] += 1
                stocks_mentioned_in_comment.add(stock_name)

                sentiment_scores = analyzer.polarity_scores(comment.body)
                stock_sentiments[stock_name].append(sentiment_scores['compound'])
                stock_comments[stock_name].append(comment.body)


                # Update timestamps
                stock_comment_timestamps[stock_name][0] = min(stock_comment_timestamps[stock_name][0],
                                                              comment.created_utc)
                stock_comment_timestamps[stock_name][1] = max(stock_comment_timestamps[stock_name][1],
                                                              comment.created_utc)

        # Increment comment count for each stock mentioned in the comment
        for stock in stocks_mentioned_in_comment:
            stock_comment_count[stock] += 1
        
        

# Aggregate the data: Create a final dictionary to combine stock mentions and sentiments
final_stock_mentions = defaultdict(int)
final_stock_sentiments = defaultdict(list)
final_stock_comment_count = defaultdict(int)
final_stock_comment_timestamps = defaultdict(lambda: [float('inf'), float('-inf')])  # [min_timestamp, max_timestamp]


# Combine mentions and sentiments for stock symbols and their company names
for symbol in stocks.keys():
    # Total mentions for both symbol and company name
    final_stock_mentions[symbol] = stock_mentions[symbol] + stock_mentions[stocks[symbol]]
    # Total comments for both symbol and company name
    final_stock_comment_count[symbol] = stock_comment_count[symbol] + stock_comment_count[stocks[symbol]]

    # Aggregate sentiments, avoiding duplicates in case both symbol and company name were mentioned
    final_stock_sentiments[symbol] = stock_sentiments[symbol] + stock_sentiments[stocks[symbol]]

    # Update min and max timestamps
    final_stock_comment_timestamps[symbol][0] = min(stock_comment_timestamps[symbol][0], stock_comment_timestamps[stocks[symbol]][0])
    final_stock_comment_timestamps[symbol][1] = max(stock_comment_timestamps[symbol][1], stock_comment_timestamps[stocks[symbol]][1])

# Function to convert Unix timestamp to readable format
def convert_unix_to_readable(timestamp):
    try:
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return ""



# Display the results
print("\n--- Stock Mention Counts ---")
for stock, count in final_stock_mentions.items():
    print(f"{stocks[stock]}: {count} mentions")

# Sort by the top 5 most mentioned stocks and display them
sorted_stock_mentions = sorted(final_stock_mentions.items(), key=lambda item: item[1], reverse=True)

print("\n--- Top 5 Stock Mentions ---")
for stock, count in sorted_stock_mentions[:5]:
    print(f"{stocks[stock]}: {count} mentions")

# Calculate and display the average sentiment for each stock
print("\n--- Average Sentiment for Each Stock ---")
for stock, sentiments in final_stock_sentiments.items():
    if sentiments:
        avg_sentiment = sum(sentiments) / len(sentiments)
        print(f"{stocks[stock]}: Average Sentiment Score = {avg_sentiment:.2f}")

# Display the number of comments mentioning each stock
print("\n--- Number of Comments per Stock ---")
for stock, count in final_stock_comment_count.items():
    print(f"{stocks[stock]}: {count} comments")

# Display the min and max comment timestamps for each stock
print("\n--- Min and Max Comment Timestamps per Stock ---")
# print(final_stock_comment_timestamps.items())
for stock, timestamps in final_stock_comment_timestamps.items():
    has_values = False

    for value in timestamps:
        if value != float('inf') and value != float('-inf'):
            has_values = True
            break
    
    if has_values:
        min_timestamp = convert_unix_to_readable(timestamps[0])
        max_timestamp = convert_unix_to_readable(timestamps[1])
        print(f"{stocks[stock]}: Min Timestamp = {min_timestamp}, Max Timestamp = {max_timestamp}")
    


# with open('C:\\Users\\Niv Junowicz\\Desktop\\stock_comments.txt', 'w') as file:
#     json.dump(stock_comments, file)