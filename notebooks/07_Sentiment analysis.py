# Databricks notebook source
!pip install nltk

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from nltk.sentiment import SentimentIntensityAnalyzer

# COMMAND ----------

# Read the data from the Gold schema
df= spark.table("silver.amazon_reviews")

# COMMAND ----------

# Step 1: Preprocessing the text data
df_cleaned = df_reviews.withColumn("cleaned_review_text", F.lower(F.regexp_replace("reviewText", "[^a-zA-Z0-9\\s]", "")))

# Step 2: sentiment analysis
# Initialize the SentimentIntensityAnalyzer
sia = SentimentIntensityAnalyzer()

# Create a UDF (User-Defined Function) to apply the sentiment analysis to each review
def analyze_sentiment(text):
    sentiment_score = sia.polarity_scores(text)
    if sentiment_score['compound'] >= 0.05:
        return 'positive'
    elif sentiment_score['compound'] <= -0.05:
        return 'negative'
    else:
        return 'neutral'

# Register the UDF and apply it to the DataFrame
spark.udf.register("analyze_sentiment_udf", analyze_sentiment)
df_with_sentiment = df_cleaned.withColumn("sentiment", F.expr("analyze_sentiment_udf(cleaned_review_text)"))

# Step 3: Analyze the results
# You can now use Databricks' DataFrame functions to perform further analysis or visualization
sentiment_distribution = df_with_sentiment.groupBy("sentiment").count()

# Display the result
display(sentiment_distribution)

# COMMAND ----------

# Select only the relevant columns for sentiment analysis
selected_df = df.select("overall", "reviewText")

# Step 1: Preprocessing the text data
selected_df= selected_df.withColumn("reviewText", F.lower(F.regexp_replace("reviewText", "[^a-zA-Z0-9\\s]", "")))

# UDF to perform sentiment analysis
nltk.download('vader_lexicon')  # Download the sentiment analyzer lexicon
sentiment_analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    sentiment_score = sia.polarity_scores(text)
    if sentiment_score['compound'] >= 0.05:
        return 'positive'
    elif sentiment_score['compound'] <= -0.05:
        return 'negative'
    else:
        return 'neutral'

# Register the UDF
analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# Apply the UDF to the DataFrame to get the sentiment label
sentiment_df = selected_df.withColumn("sentiment", analyze_sentiment_udf("reviewText"))




# COMMAND ----------

# Step 3: Analyze the results
# You can now use Databricks' DataFrame functions to perform further analysis or visualization
sentiment_distribution = df_with_sentiment.groupBy("sentiment").count()

# Show the resulting DataFrame with the sentiment label
display(sentiment_df)
