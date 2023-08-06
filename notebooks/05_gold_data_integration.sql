-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import DataFrame
-- MAGIC from pyspark.sql.functions import col, count, when,round,explode, split, regexp_extract, collect_list,expr,when, year, month,coalesce,size
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from datetime import datetime, timedelta
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.types import IntegerType, StringType,FloatType
-- MAGIC import string
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC #import nltk
-- MAGIC #from nltk.corpus import stopwords
-- MAGIC #from nltk.tokenize import word_tokenize
-- MAGIC import re
-- MAGIC import html

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.amazon_reviews
USING DELTA
AS
SELECT * FROM silver.amazon_reviews
UNION ALL
SELECT * FROM silver.reviews_streaming;

-- COMMAND ----------

CREATE TABLE gold.reviews_details;
ALTER TABLE gold.reviews_details SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

-- COMMAND ----------

INSERT INTO gold.reviews_details
SELECT 
r.*, m.also_buy, m.also_view, m.brand, m.category, m.date, m.description, m.details, m.feature, m.main_cat, m.price, m.rank as rank_, m.similar_item, m.title
FROM gold.amazon_reviews AS r
LEFT JOIN silver.amazon_metadata AS m
ON r.asin = m.asin;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("gold.reviews_details")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Calculate the length of the review text in words
-- MAGIC df = df.withColumn('review_length_words', size(split(col('reviewText'), ' ')))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.withColumn("Month", month(col("unixReviewTime")))
-- MAGIC df = df.withColumn("Year", year(col("unixReviewTime")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def unescape_html(text):
-- MAGIC     if text is not None:
-- MAGIC         return html.unescape(text)
-- MAGIC
-- MAGIC unescape_udf = udf(unescape_html, StringType())
-- MAGIC
-- MAGIC df = df.withColumn("main_cat", when(col("main_cat").isNull(), "undefined").otherwise(unescape_udf(col("main_cat"))))
-- MAGIC
-- MAGIC df = df.withColumn("extracted_cat", when(col("main_cat")\
-- MAGIC     .like('<img src=%'), regexp_extract(col("main_cat"), 'alt="([^"]+)"', 1)).otherwise(col("main_cat")))
-- MAGIC
-- MAGIC df = df.withColumn("extracted_cat",F.when(F.col("extracted_cat")=="undefined", F.col('category').getItem(0))\
-- MAGIC     .otherwise(F.col("extracted_cat")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Define a function to calculate the average price
-- MAGIC def calculate_average_price(price):
-- MAGIC
-- MAGIC     if price is not None:
-- MAGIC         # Regular expression to match price ranges
-- MAGIC         range_pattern = r"\$(\d+(?:\.\d+)?)\s*-\s*\$(\d+(?:\.\d+)?)"
-- MAGIC         if "-" in price:
-- MAGIC             range_match = re.match(range_pattern, price)
-- MAGIC             if range_match:
-- MAGIC                 try:
-- MAGIC                     min_price = float(range_match.group(1))
-- MAGIC                     max_price = float(range_match.group(2))
-- MAGIC                     avg_price = (min_price + max_price) / 2
-- MAGIC                     return avg_price
-- MAGIC                 except (ValueError, IndexError):
-- MAGIC                     return None
-- MAGIC         elif "$" in price:
-- MAGIC             try:
-- MAGIC                 return float(price.strip('$'))
-- MAGIC             except ValueError:
-- MAGIC                 return None
-- MAGIC         
-- MAGIC         elif price=="":
-- MAGIC             return None

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Register the function as a UDF (User-Defined Function)
-- MAGIC calculate_avg_price_udf = F.udf(calculate_average_price, FloatType())
-- MAGIC
-- MAGIC df = df.withColumn("average_price", calculate_avg_price_udf(F.col("price")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Step 1: Calculate the mean for each "main_cat" category
-- MAGIC mean_df = df.groupBy("main_cat").agg(F.round(F.avg("average_price"),2).alias("mean_price"))
-- MAGIC
-- MAGIC # Step 2: Join the original DataFrame with the mean DataFrame to get the mean_price for each main_cat
-- MAGIC df = df.join(mean_df, on="main_cat", how="left")
-- MAGIC
-- MAGIC # Step 3: Replace null values in "average_price" column with the corresponding mean_price
-- MAGIC df = df.withColumn("average_price", F.when(F.col("average_price").isNull(), F.col("mean_price"))
-- MAGIC     .otherwise((F.col("average_price"))))
-- MAGIC
-- MAGIC #mean_value_total = df.selectExpr("mean(average_price)").collect()[0][0]
-- MAGIC
-- MAGIC # Paso 4: impute the rest of null values with the total average 
-- MAGIC #df = df.withColumn("average_price", col("average_price").fillna(mean_value_total))
-- MAGIC
-- MAGIC # Step 5: Drop the "mean_price" column, as it's no longer needed
-- MAGIC df = df.drop("mean_price")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df= df.withColumn("brand",when((col("brand").isNull()) | (col("brand") == "") | (col("brand") == "Unknown"), "Unknown")
-- MAGIC                                   .otherwise(col("brand")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pattern = r'^<span.*?>(.*?)</span>'
-- MAGIC df=df.withColumn("title", when(col("title")\
-- MAGIC     .like('<span id=%'), regexp_extract(col("title"), pattern, 1)).otherwise(col("title")))
-- MAGIC
-- MAGIC df= df.withColumn("title",when((col("title").isNull()) | (col("title") == "") , "Unknown")
-- MAGIC                                   .otherwise(col("title")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.drop("details")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS gold.amazon_reviews_gold;
-- MAGIC ALTER TABLE gold.amazon_reviews_gold SET TBLPROPERTIES (
-- MAGIC    'delta.columnMapping.mode' = 'name',
-- MAGIC    'delta.minReaderVersion' = '2',
-- MAGIC    'delta.minWriterVersion' = '5')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold.amazon_reviews_gold")

-- COMMAND ----------

SELECT*
FROM gold.amazon_reviews_gold
LIMIT 10

-- COMMAND ----------

SELECT COUNT(*)
FROM gold.amazon_reviews_gold
