# Databricks notebook source
# MAGIC %md
# MAGIC #Data cleaning and feature extraction
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when,round,explode, split, regexp_extract, collect_list,expr,when, year, month,coalesce,size
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType,FloatType
import string

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
#import nltk
#from nltk.corpus import stopwords
#from nltk.tokenize import word_tokenize
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframes

# COMMAND ----------

# Reading the data from the Gold schema
df_reviews = spark.table("gold.reviews_details")
#df_reviews_streaming=spark.table("silver.amazon_reviews_stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Amazon reviews

# COMMAND ----------

display(df_reviews)

# COMMAND ----------

total_rows_reviews = df_reviews.count()
print("Number of rows in the DataFrame of amazon reviews: ", total_rows_reviews)

# COMMAND ----------

total_rows_reviews_streaming = df_reviews_streaming.count()
print("Number of rows in the DataFrame of amazon reviews: ", total_rows_reviews_streaming)

# COMMAND ----------

# how many unique products
unique_product= df_reviews.select("asin").distinct().count()
print("Number of unique products in the DataFrame of amazon reviews: ", unique_product)
# how many unique reviewText per user
unique_reviews = df_reviews.select("reviewText").distinct().count()
print("Number of unique reviews in the DataFrame of amazon reviews: ", unique_reviews)
# how many unique reviewText per user
unique_users = df_reviews.select("reviewerID").distinct().count()
print("Number of unique users in the DataFrame of amazon reviews: ", unique_users)

# COMMAND ----------

# how many unique products
unique_product= df_reviews_streaming.select("asin").distinct().count()
print("Number of unique products in the DataFrame of amazon reviews streaming: ", unique_product)
# how many unique reviewText per user
unique_reviews = df_reviews_streaming.select("reviewText").distinct().count()
print("Number of unique reviews in the DataFrame of amazon reviews streaming: ", unique_reviews)
# how many unique reviewText per user
unique_users = df_reviews_streaming.select("reviewerID").distinct().count()
print("Number of unique users in the DataFrame of amazon reviews streaming: ", unique_users)

# COMMAND ----------

# MAGIC %md
# MAGIC We ave 2 dataset with the same kind of information so we are going to join them and get just one dataset

# COMMAND ----------

# Perform vertical concatenation (union) using the union method
df_reviews_final = df_reviews.union(df_reviews_streaming)

# COMMAND ----------

# how many unique products
unique_product= df_reviews_final.select("asin").distinct().count()
print("Number of unique products in the DataFrame of amazon reviews streaming: ", unique_product)
# how many unique reviewText per user
unique_reviews = df_reviews_final.select("reviewText").distinct().count()
print("Number of unique reviews in the DataFrame of amazon reviews streaming: ", unique_reviews)
# how many unique reviewText per user
unique_users = df_reviews_final.select("reviewerID").distinct().count()
print("Number of unique users in the DataFrame of amazon reviews streaming: ", unique_users)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking duplicated rows

# COMMAND ----------

# Create a Window specification to partition by 'user' and order by 'reviewText'
window_spec = Window.partitionBy("reviewerID").orderBy("reviewText")

# Add a new column 'duplicate_count' that counts the occurrences of each reviewText within each user
df_with_duplicates = df_reviews.withColumn("duplicate_count", F.count("reviewText").over(window_spec))

# Filter out rows where 'duplicate_count' is greater than 1 to find duplicate reviews for the same user
duplicate_user_reviews_df = df_with_duplicates.filter(col("duplicate_count") > 1)

#  Drop duplicates and keep only distinct rows for the duplicates
# Assuming df is your DataFrame
distinct_duplicate_user_reviews_df = duplicate_user_reviews_df.select("asin","reviewerID","reviewText","unixReviewTime","duplicate_count").dropDuplicates()

# Step 5: Show the DataFrame containing duplicate reviews for the same user
print("Duplicate Reviews for the Same User:")
display(distinct_duplicate_user_reviews_df)


# COMMAND ----------

# MAGIC %md
# MAGIC So we can notice that we have a lot of duplicated reviews so we are going to delete these duplicated rows

# COMMAND ----------

# Drop duplicates based on 'reviewerID', 'asin', and 'reviewText'
df_no_duplicates = df_reviews.dropDuplicates(subset=['reviewerID', 'asin', 'reviewText',"unixReviewTime","overall","summary"])
total_rows_reviews = df_no_duplicates.count()
print("Number of rows in the DataFrame of amazon reviews: ", total_rows_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Null values

# COMMAND ----------

def calculate_nulls(df: DataFrame) -> DataFrame:
    """
    Calculate the percentage of null values for each column in a Spark DataFrame.
    Args:
        df (DataFrame): The Spark DataFrame.
    Returns:
        DataFrame: A DataFrame containing the percentage of null values for each column in the input DataFrame.
    """
    # total number of rows in the DataFrame
    total_rows = df.count()
    # count of null values for each column
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    # percentage of null values for each column
    null_percentages = null_counts.select([round(((col(c) / total_rows) * 100), 3).alias(c) for c in df.columns])

    melted_df = null_percentages.selectExpr("stack({}, {}) as (Column, Percentage_Null)"\
                .format(len(df.columns), ", ".join([f"'{c}', {c}" for c in df.columns])))
    
    return melted_df


# COMMAND ----------

null_reviews=calculate_nulls(df_no_duplicates)
display(null_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC  we can see that there are columns with a high percentage of null values, for which we will eliminate those columns that have a percentage greater than 70% of null values, the other columns that contain nulls we will look for the best way to impute these values

# COMMAND ----------

def delete_nulls(df: DataFrame,null_threshold: float = 70.0) -> DataFrame:
    """
    This function eliminates the columns of the dataframe that have a percentage of null values greater than 70%.
    Args:
        df (DataFrame): The Spark DataFrame.
        null_threshold (float, optional): The threshold percentage of null values. Default is 70.0.
    Returns:
        DataFrame: Final Dataframe after removing columns with high percentages of null values.
    """
    # Calculate the null percentages for each column using the calculate_nulls function
    null_percentages_df = calculate_nulls(df)

    # Filter out the columns that have null percentage less than or equal to the threshold
    columns_to_keep = null_percentages_df.filter(col("Percentage_Null") <= null_threshold).select("Column").rdd.flatMap(lambda x: x).collect()

    # Select only the columns that passed the threshold
    result_df = df.select(*columns_to_keep)

    return result_df


# COMMAND ----------

new_dfrev=delete_nulls(df_no_duplicates)

# COMMAND ----------

new_dfrev=df_no_duplicates.select("asin","reviewText","overall","also_view","also_buy","main_cat","brand",
                                  "title","verified","unixReviewTime","reviewerID","category","price","summary")

# COMMAND ----------

# MAGIC %md
# MAGIC We are also going to impute the null values in the column reviewText with the information of the summary column, and if both columns are nulls then we are going to delete it 

# COMMAND ----------

# Impute 'reviewText' with 'summary' column if 'reviewText' is null
df_imputed = new_dfrev.withColumn("reviewText", coalesce(col("reviewText"), col("summary")))

# Drop rows where both 'reviewText' and 'summary' are null
df_cleaned_review = df_imputed.dropna(subset=["reviewText", "summary"])

# Show the DataFrame after imputing and removing null rows
#null_reviews=calculate_nulls(df_cleaned_review)
#display(null_reviews)

# COMMAND ----------

df_cleaned_review.write \
    .format("delta") \
    .mode("append") \
    .option("overwriteSchema", "true") \
    .save("s3://1-factored-datathon-2023-lakehouse/gold/review_cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Feature extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ######1. Number of stopwords

# COMMAND ----------

nltk.download('stopwords')
stop = stopwords.words('english')

def remove_stopwords(text):
    return len([word for word in text.split() if word.lower() not in stop])

remove_stopwords_udf = udf(remove_stopwords, IntegerType())

df_cleaned_review= df_cleaned_review.withColumn('stopwords', remove_stopwords_udf(col('reviewText')))


# COMMAND ----------

# MAGIC %md
# MAGIC ######2. Number of Punctuation

# COMMAND ----------

# Function to count punctuation
def count_punct(text):
    return sum(1 for char in text if char in string.punctuation)

# Register UDF
count_punct_udf = udf(count_punct, IntegerType())

# Apply UDF to calculate the number of punctuation marks in each text
df_cleaned_review = df_cleaned_review.withColumn('punctuation', count_punct_udf(col('reviewText')))
#display(df_cleaned_review.select("reviewText","punctuation","stopwords"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Text cleaning techniques

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Make all text lower case
# MAGIC
# MAGIC The first pre-processing step which we will do is transform our reviews into lower case. This avoids having multiple copies of the same words. For example, while calculating the word count, ‘Analytics’ and ‘analytics’ will be taken as different words

# COMMAND ----------

# Function to convert text to lowercase and join
def lowercase_and_join(text):
    return " ".join(word.lower() for word in text.split())

# Register UDF
lowercase_and_join_udf = udf(lowercase_and_join, StringType())

# Apply UDF to convert the text to lowercase and join it back
df_cleaned_review= df_cleaned_review.withColumn('lowerReviewText', lowercase_and_join_udf(col('reviewText')))


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing punctuation

# COMMAND ----------

# Function to remove non-word characters using regex
def remove_special_characters(text):
    return re.sub(r'[^\w\s]', '', text)

# Register UDF
remove_special_characters_udf = udf(remove_special_characters, StringType())

# Apply UDF to remove non-word characters from the 'Text' column
df_cleaned_review = df_cleaned_review.withColumn('lowerReviewText', remove_special_characters_udf(col('lowerReviewText')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing Stop Words

# COMMAND ----------

# Function to remove stopwords
def remove_stopwords(text):
    return " ".join(word for word in text.split() if word.lower() not in stop)

# Register UDF
remove_stopwords_udf = udf(remove_stopwords, StringType())

# Apply UDF to remove stopwords from the 'Text' column
df_cleaned_review = df_cleaned_review.withColumn('finalReviewText', remove_stopwords_udf(col("lowerReviewText")))
#df_cleaned_review.select('finalReviewText').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing URLs

# COMMAND ----------

# Function to remove URLs using regex
def remove_url(text):
    url = re.compile(r'https?://\S+|www\.\S+')
    return url.sub(r'', text)

# Register UDF
remove_url_udf = udf(remove_url, StringType())

# Apply UDF to remove URLs from the 'Text' column
df_cleaned_review = df_cleaned_review.withColumn("finalReviewText", remove_url_udf(col("finalReviewText")))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Remove html tags

# COMMAND ----------

# Function to remove HTML tags using regex
def remove_html(text):
    html = re.compile(r'<.*?>')
    return html.sub(r'', text)

# Register UDF
remove_html_udf = udf(remove_html, StringType())

# Apply UDF to remove HTML tags from the 'Text' column
df_cleaned_review= df_cleaned_review.withColumn("finalReviewText", remove_html_udf(col("finalReviewText")))


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing emojis

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
df_cleaned_review = df_cleaned_review.withColumn("finalReviewText", remove_emoji_udf(col("finalReviewText")))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing emoticons

# COMMAND ----------

# Function to remove emoticons using regex
def remove_emoticons(text):
    emoticon_pattern = re.compile(r'(?::|;|=)(?:-)?(?:\)|\(|D|P)')
    return emoticon_pattern.sub(r'', text)

# Register UDF
remove_emoticons_udf = udf(remove_emoticons, StringType())

# Apply UDF to remove emoticons from the 'Text' column
df_cleaned_review= df_cleaned_review.withColumn("finalReviewText", remove_emoticons_udf(col("finalReviewText")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic Feature Extraction - 2

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Number of Words 
# MAGIC

# COMMAND ----------

# Calculate the length of the review text in words
df_cleaned_review = df_cleaned_review.withColumn('review_length_words', size(split(col('reviewText'), ' ')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Number of characters

# COMMAND ----------

df_cleaned_review=df_cleaned_review.withColumn('review_length_chars', F.length('reviewText'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Average Word Length
# MAGIC

# COMMAND ----------

# Function to calculate average word length
def avg_word(sentence):
    words = sentence.split()
    return sum(len(word) for word in words) / (len(words) + 0.000001)

# Register UDF
avg_word_udf = udf(avg_word, FloatType())

# Apply UDF to calculate the average word length for each text
df_cleaned_review = df_cleaned_review.withColumn('avg_word', avg_word_udf(col('reviewText')))
df_cleaned_review = df_cleaned_review.withColumn('avg_word', round(col('avg_word'), 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Month and year columns

# COMMAND ----------

# Create 'Month' and 'Year' columns from 'Date'
df_cleaned_review = df_cleaned_review.withColumn("Month", month(col("unixReviewTime")))
df_cleaned_review = df_cleaned_review.withColumn("Year", year(col("unixReviewTime")))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Columns to delete

# COMMAND ----------

df_cleaned_review.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC we have several columns that we can omit for our analysis, for example, we do not need the column reviewerName since wealready have a reviewerID column that refers to the users

# COMMAND ----------

final_df=df_cleaned_review.drop("reviewerName")

# COMMAND ----------

display(final_df)

# COMMAND ----------

df_cleaned_review.write \
    .format("delta") \
    .mode("append") \
    .option("overwriteSchema", "true") \
    .save("s3://1-factored-datathon-2023-lakehouse/gold/review_cleaned2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Amazon metadata

# COMMAND ----------

# Reading the data from the Gold schema
df_metadata = spark.table("silver.amazon_metadata")
display(df_metadata)

# COMMAND ----------

# how many unique products
unique_product= df_metadata.select("asin").distinct().count()
print("Number of unique products in the DataFrame of amazon metadata: ", unique_product)
# how many unique brands 
unique_brands = df_metadata.select("brand").distinct().count()
print("Number of unique brands in the DataFrame of amazon metadata: ", unique_reviews)
# how many unique categories
unique_users = df_metadata.select("main_cat").distinct().count()
print("Number of unique categories in the DataFrame of amazon metadata: ", unique_users)

# COMMAND ----------

total_rows_metadata = df_metadata.count()
print("Number of rows in the DataFrame of amazon metadata: ", total_rows_metadata)

# COMMAND ----------

null_metadata=calculate_nulls(df_metadata)
display(null_metadata)
