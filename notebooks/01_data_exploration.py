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

# MAGIC %md 
# MAGIC #### Data imputation - main_cat

# COMMAND ----------

def unescape_html(text):
    if text is not None:
        return html.unescape(text)

unescape_udf = udf(unescape_html, StringType())

new_df = union_df.withColumn("main_cat", when(col("main_cat").isNull(), "undefined").otherwise(unescape_udf(col("main_cat"))))
#new_df= union_df.withColumn("main_cat", unescape_udf(col("main_cat")))

new_df = new_df.withColumn("extracted_cat", when(col("main_cat")\
    .like('<img src=%'), regexp_extract(col("main_cat"), 'alt="([^"]+)"', 1)).otherwise(col("main_cat")))

new_df = new_df.withColumn("extracted_cat",F.when(F.col("extracted_cat")=="undefined", F.col('category').getItem(0))\
    .otherwise(F.col("extracted_cat")))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Data imputation - price

# COMMAND ----------

# Define a function to calculate the average price
def calculate_average_price(price):

    if price is not None:
        # Regular expression to match price ranges
        range_pattern = r"\$(\d+(?:\.\d+)?)\s*-\s*\$(\d+(?:\.\d+)?)"
        if "-" in price:
            range_match = re.match(range_pattern, price)
            if range_match:
                try:
                    min_price = float(range_match.group(1))
                    max_price = float(range_match.group(2))
                    avg_price = (min_price + max_price) / 2
                    return avg_price
                except (ValueError, IndexError):
                    return None
        elif "$" in price:
            try:
                return float(price.strip('$'))
            except ValueError:
                return None
        
        elif price=="":
            return None
    
# Register the function as a UDF (User-Defined Function)
calculate_avg_price_udf = F.udf(calculate_average_price, FloatType())

# Create a new column "average_price" using the UDF to calculate the average price
#df =new_df.withColumn("average_price", when(col("price").isNull(), 0).otherwise(calculate_avg_price_udf(F.col("price"))))
df = new_df.withColumn("average_price", calculate_avg_price_udf(F.col("price")))

# COMMAND ----------

# Step 1: Calculate the mean for each "main_cat" category
mean_df = df.groupBy("main_cat").agg(F.round(F.avg("average_price"),2).alias("mean_price"))

# Step 2: Join the original DataFrame with the mean DataFrame to get the mean_price for each main_cat
df = df.join(mean_df, on="main_cat", how="left")

# Step 3: Replace null values in "average_price" column with the corresponding mean_price
df = df.withColumn("average_price", F.when(F.col("average_price").isNull(), F.col("mean_price"))
    .otherwise((F.col("average_price"))))

#mean_value_total = df.selectExpr("mean(average_price)").collect()[0][0]

# Paso 4: impute the rest of null values with the total average 
#df = df.withColumn("average_price", col("average_price").fillna(mean_value_total))

# Step 5: Drop the "mean_price" column, as it's no longer needed
df = df.drop("mean_price")


# COMMAND ----------

# MAGIC %md
# MAGIC #### impute column brand

# COMMAND ----------

# Use when to replace null, empty strings, and "unknown" values with a default value (e.g., "NA")
df= df.withColumn("brand",when((col("brand").isNull()) | (col("brand") == "") | (col("brand") == "Unknown"), "Unknown")
                                  .otherwise(col("brand")))

# Use concat_ws to concatenate the values into a single column
#df= df.withColumn("brand", concat_ws(", ", col("brand")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Column title - format

# COMMAND ----------

pattern = r'^<span.*?>(.*?)</span>'
df=df.withColumn("title", when(col("title")\
    .like('<span id=%'), regexp_extract(col("title"), pattern, 1)).otherwise(col("title")))

df= df.withColumn("title",when((col("title").isNull()) | (col("title") == "") , "Unknown")
                                  .otherwise(col("title")))
#df = df.withColumn("title", regexp_extract(col("title"), pattern, 1))
