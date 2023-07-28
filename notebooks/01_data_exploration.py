# Databricks notebook source
# MAGIC %md
# MAGIC #Exploratory Data Analysis
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframes

# COMMAND ----------

# Read the data from the Gold schema
df_reviews = spark.table("bronze.amazon_reviews")
df_metadata = spark.table("bronze.amazon_metadata")

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
    null_percentages = null_counts.select([((col(c) / total_rows) * 100).alias(f"{c}_null_percentage") for c in df.columns])

    return null_percentages


# COMMAND ----------

null_reviews=calculate_nulls(df_reviews)
display(null_reviews)
