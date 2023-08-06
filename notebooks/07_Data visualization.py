# Databricks notebook source
df = spark.table("gold.amazon_reviews_gold")

# COMMAND ----------

!pip install nltk

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import lower,col,regexp_replace,when,rand,regexp_extract,length
from pyspark.sql.types import IntegerType, StringType,FloatType
import re
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
import html

from nltk.corpus import stopwords
import nltk
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF, Tokenizer


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of overall rating

# COMMAND ----------

# Count the occurrences of each rating value
rating_counts = df.groupBy('overall').count().orderBy('overall')
display(rating_counts)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Relationship between verified purchases and overall ratings

# COMMAND ----------

# Filter data for verified purchases with 'overall' rating
verified_purchases = df.select('verified','overall')

verified_purchases =verified_purchases.groupBy("overall", "verified").count().orderBy("overall")
# Show the DataFrame with both verified and not verified purchases and their 'overall' ratings
display(verified_purchases)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of review lenghts in words

# COMMAND ----------

# Calculate the length of each review in words and create a new column 'review_length_words'
display(df.select('review_length_words'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Average of ratings per year

# COMMAND ----------

#average ratings grouped by 'reviewTime')
averageRatingsByDate = df\
    .groupBy("year") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("year")

# Display the Results
display(averageRatingsByDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Average of ratings per month

# COMMAND ----------

averageRatingsByMonth = df\
    .groupBy("month") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("month")

# Display the Results
display(averageRatingsByMonth)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reviews per year

# COMMAND ----------

ReviewsYear = df\
    .groupBy("year").count().orderBy("year")

# Display the Results
display(ReviewsYear)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reviews per month

# COMMAND ----------

ReviewsMonth = df\
    .groupBy("month").count()

# Display the Results
display(ReviewsMonth)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sentiment analysis

# COMMAND ----------

# Add a new column "sentiment_label" based on sentiment polarity
df = df.withColumn("sentiment", when(col("overall") < 3.0, "Negative")
                                   .when(col("overall") == 3.0, "Neutral")
                                   .when(col("overall") > 3.0, "Positive"))
display(df.groupBy('sentiment').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Relationship between length of the review and sentiment

# COMMAND ----------

display(df.select('review_length_words', 'sentiment'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sentiment per year

# COMMAND ----------

display(df.select("sentiment","Year"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Total reviews per main cat

# COMMAND ----------

df_grouped = df.groupBy("extracted_cat").agg(F.count("reviewText").alias("review_count"))

# Order the DataFrame by the count in descending order
df_sorted = df_grouped.orderBy(F.desc("review_count"))
display(df_sorted.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Overall average per category

# COMMAND ----------

averageRatingsByMainCat = df\
    .groupBy("extracted_cat") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy(F.asc("averageRating"))

# Display the Results
display(averageRatingsByMainCat.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Distribution of price

# COMMAND ----------

display(df.select("average_price"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Negative reviews analysis

# COMMAND ----------

negative_reviews_df = df.filter((F.col("overall") < 3))
display(negative_reviews_df.groupBy("extracted_cat").count().orderBy(F.desc("count")).limit(10))

# COMMAND ----------

count_brand=negative_reviews_df.groupBy("brand").count().orderBy(F.desc("count"))
top_10_products_n = count_brand.limit(10)
display(top_10_products_n)

# COMMAND ----------

count_title=negative_reviews_df.groupBy("title").count().orderBy(F.desc("count"))
top_10_products_n = count_title.limit(10)
display(top_10_products_n)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Positive reviews analysis

# COMMAND ----------

positive_reviews_df = df.filter((col("overall") > 3))
display(positive_reviews_df.groupBy("extracted_cat").count().orderBy(F.desc("count")).limit(10))

# COMMAND ----------

count_brand_p=positive_reviews_df.groupBy("brand").count().orderBy(F.desc("count"))
top_10_products_p = count_brand_p.limit(10)
display(top_10_products_p)

# COMMAND ----------

count_title_p=positive_reviews_df.groupBy("title").count().orderBy(F.desc("count"))
top_10_products_p= count_title_p.limit(10)
display(top_10_products_p)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Sentiment analysis
# MAGIC

# COMMAND ----------

# Group by 'sentiment' and count the occurrences
sentiment_counts = df.groupBy('sentiment').count()

# Calculate the total count for each sentiment label
total_counts = df.groupBy('sentiment').agg({'sentiment': 'count'})

# Rename the 'count' column to 'total_count'
total_counts = total_counts.withColumnRenamed('count(sentiment)', 'total_count')

# Create a new DataFrame with the 'Total' sentiment and the total count
total_row = spark.createDataFrame([("Total", total_counts.groupBy().sum('total_count').collect()[0][0])], ["sentiment", "total_count"])

# Union the original DataFrame with the total_row DataFrame
result_df = sentiment_counts.union(total_row)
display(result_df)

# COMMAND ----------

counter_users=df.groupBy("reviewerID").count().orderBy(F.desc("count"))
display(counter_users)

# COMMAND ----------

# Total number of reviews
total_reviews = df.count()

# Total number of unique products (asin)
total_unique_products = df.select("asin").distinct().count()

# Total number of unique users (reviewerId)
total_unique_users = df.select("reviewerID").distinct().count()

# Creating a new DataFrame with the desired statistics
statistics_df = spark.createDataFrame([
    (total_reviews, total_unique_products, total_unique_users)
], ["Total Reviews", "Total Unique Products", "Total Unique Users"])

# COMMAND ----------

display(statistics_df)

# COMMAND ----------

display(counter_users.limit(10))

# COMMAND ----------

display(df.select("overall","Year","sentiment"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wordcloud

# COMMAND ----------

dfword=df.select("reviewText")
# Drop rows with null or NaN values in the 'reviewText' column
dfword = dfword.dropna(subset=["reviewText"])

# COMMAND ----------

# Function to remove non-word characters using regex
def remove_special_characters(text):
    return re.sub(r'[^\w\s]', '', text)

# Register UDF
remove_special_characters_udf = udf(remove_special_characters, StringType())

# Apply UDF to remove non-word characters from the 'Text' column
df_cleaned_review = dfword.withColumn('cleanedReviewText', remove_special_characters_udf(col('reviewText')))

# COMMAND ----------

# Function to remove HTML tags using regex
def remove_html(text):
    cleanr = re.compile('<.*?>')
    cleanr2 = re.compile('<.*?</a>')
    textm=re.sub(cleanr2, '', text)
    textf=re.sub(cleanr, '', textm)
    return textf

# Register UDF
remove_html_udf = udf(remove_html, StringType())

# Apply UDF to remove HTML tags from the 'Text' column
df_cleaned_review= df_cleaned_review.withColumn("cleanedReviewText", remove_html_udf(col("cleanedReviewText")))

# COMMAND ----------

# Function to convert text to lowercase and join
def lowercase_and_join(text):
    return " ".join(word.lower() for word in text.split())

# Register UDF
lowercase_and_join_udf = udf(lowercase_and_join, StringType())

# Apply UDF to convert the text to lowercase and join it back
df_cleaned_review= df_cleaned_review.withColumn('cleanedReviewText', lowercase_and_join_udf(col('cleanedReviewText')))

# COMMAND ----------

# Function to remove URLs using regex
def remove_url(text):
    url = re.compile(r'https?://\S+|www\.\S+')
    return url.sub(r'', text)

# Register UDF
remove_url_udf = udf(remove_url, StringType())

# Apply UDF to remove URLs from the 'Text' column
df_cleaned_review = df_cleaned_review.withColumn("cleanedReviewText", remove_url_udf(col("cleanedReviewText")))

# COMMAND ----------

# Function to remove emojis using regex
def remove_emoji(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    
    return emoji_pattern.sub(r'', text)

# Register UDF
remove_emoji_udf = udf(remove_emoji, StringType())

# Apply UDF to remove emojis from the 'Text' column
df_cleaned_review = df_cleaned_review.withColumn("cleanedReviewText", remove_emoji_udf(col("cleanedReviewText")))

# COMMAND ----------

def remove_emoticons(text):
    emoticon_pattern = re.compile(r'(?::|;|=)(?:-)?(?:\)|\(|D|P)')
    return emoticon_pattern.sub(r'', text)

# Register UDF
remove_emoticons_udf = udf(remove_emoticons, StringType())

# Apply UDF to remove emoticons from the 'Text' column
df_cleaned_review= df_cleaned_review.withColumn("cleanedReviewText", remove_emoticons_udf(col("cleanedReviewText")))

# COMMAND ----------

# Use regexp_replace to remove numerical characters from the 'text_column'
df_cleaned_review= df_cleaned_review.withColumn("cleanedReviewText",regexp_replace(col("cleanedReviewText"), r'\d+', ''))
# Remove rows with cleanedReviewText as "" or with less than 4 characters
df_cleaned_review = df_cleaned_review.filter((length(col("cleanedReviewText")) >= 4) & (col("cleanedReviewText") != ""))
#df=df_cleaned_review.select("cleanedReviewText","label")

# COMMAND ----------

# Tokenize the review. 
tokenizer = Tokenizer(inputCol="cleanedReviewText", outputCol="review_words")
wordsDF = tokenizer.transform(df_cleaned_review)
#wordsDF.show(2)

# COMMAND ----------

# Remove stop words
remover = StopWordsRemover(inputCol="review_words", outputCol="filtered")
wordsDF2 = remover.transform(wordsDF)
#wordsDF2.show(10)

# COMMAND ----------

from pyspark.sql.functions import explode, count

new_df =wordsDF2.select(explode("filtered").alias("word")) \
           .groupBy("word") \
           .agg(count("*").alias("frequency"))


# COMMAND ----------

df_wordcloud=new_df.orderBy(F.desc("frequency"))
display(df_wordcloud.limit(3000))
