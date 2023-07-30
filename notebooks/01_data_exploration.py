# Databricks notebook source
# MAGIC %md
# MAGIC #Exploratory Data Analysis
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when,round,explode, split, regexp_extract, collect_list,expr,when, year, month
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframes

# COMMAND ----------

# Reading the data from the Gold schema
df_reviews = spark.table("silver.amazon_reviews")
df_metadata = spark.table("silver.amazon_metadata")

# COMMAND ----------

total_rows_reviews = df_reviews.count()
total_rows_metadata = df_metadata.count()
print("Number of rows in the DataFrame of amazon reviews: ", total_rows_reviews)
print("Number of rows in the DataFrame of amazon metadata: ", total_rows_metadata)

# COMMAND ----------

display(df_reviews)

# COMMAND ----------

display(df_metadata)

# COMMAND ----------

distinct_keys

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

null_reviews=calculate_nulls(df_reviews)
display(null_reviews)

# COMMAND ----------

null_metadata=calculate_nulls(df_metadata)
display(null_metadata)

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

new_dfrev=delete_nulls(df_reviews)

# COMMAND ----------

new_dfmet=delete_nulls(df_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Analysis DF Amazon Reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of overall rating

# COMMAND ----------

# Count the occurrences of each rating value
rating_counts = new_dfrev.groupBy('overall').count().orderBy('overall')
display(rating_counts)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Relationship between verified purchases and overall ratings

# COMMAND ----------

# Filter data for verified purchases with 'overall' rating
verified_purchases = new_dfrev.select('verified','overall')

# Show the DataFrame with both verified and not verified purchases and their 'overall' ratings
display(verified_purchases)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of review lenghts in characters

# COMMAND ----------

# Calculate the length of each review in characters
new_dfrev = new_dfrev.withColumn('review_length_chars', F.length('reviewText'))
display(new_dfrev.select('review_length_chars'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of review lenghts in words

# COMMAND ----------

# Calculate the length of each review in words and create a new column 'review_length_words'
new_dfrev = new_dfrev.withColumn('review_length_words', F.size(F.split(new_dfrev['reviewText'], ' ')))
display(new_dfrev.select('review_length_words'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Relationship between length of the review and overall ratings

# COMMAND ----------

# Collect the data to the driver as Pandas DataFrames for plotting
new_dfrev.select('review_length_words', 'overall')
display(new_dfrev.select('review_length_words', 'overall'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Time series analysis

# COMMAND ----------

#average ratings grouped by 'reviewTime')
averageRatingsByDate = new_dfrev \
    .groupBy("unixReviewTime") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("unixReviewTime")

# Display the Results
display(averageRatingsByDate)


# COMMAND ----------

# Extract year and month from the 'unixReviewTime' column
averageRatingsByDate = new_dfrev.withColumn("year", year("unixReviewTime")).withColumn("month", month("unixReviewTime"))

# Group by year and month and calculate the average rating
averageRatingsByMonthYear = averageRatingsByDate \
    .groupBy("year", "month") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("year", "month")

# Get the last year from the DataFrame
last_year = averageRatingsByMonthYear.select("year").distinct().orderBy("year", ascending=False).limit(1).collect()[0]["year"]

# Filter the data to include only the last five years
averageRatingsLastFiveYears = averageRatingsByMonthYear.filter((averageRatingsByDate["year"] >= last_year-4) & (averageRatingsByMonthYear["year"] <= last_year))

display(averageRatingsLastFiveYears)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Analysis DF Amazon Metadata

# COMMAND ----------

display(df_streaming)
