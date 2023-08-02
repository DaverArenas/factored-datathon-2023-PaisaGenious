# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime, to_date, split, from_utc_timestamp, from_json, date_format, to_timestamp, current_timestamp
from pyspark.sql.types import MapType, StringType, DecimalType

# COMMAND ----------

# MAGIC %sql
# MAGIC USE silver;
# MAGIC CREATE TABLE IF NOT EXISTS amazon_reviews_stream(
# MAGIC     asin                STRING NOT NULL,
# MAGIC     image               STRING,
# MAGIC     overall             Decimal(10,1),
# MAGIC     reviewText          STRING,
# MAGIC     reviewerID          STRING,
# MAGIC     reviewerName        STRING,
# MAGIC     style               MAP<STRING, STRING>,
# MAGIC     summary             STRING,
# MAGIC     unixReviewTime      DATE,
# MAGIC     verified            STRING,
# MAGIC     vote                STRING,
# MAGIC     processing_time     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# Define the base path for S3 bucket
s3_base_path = "s3://1-factored-datathon-2023-lakehouse"
silver_s_reviews = 'silver.amazon_reviews_stream'
checkpoint_reviews_s_silver = s3_base_path + '/Silver/amazon_reviews_stream/'

# COMMAND ----------

silver_amazon_s_reviews = spark.readStream \
                            .table("Bronze.amazon_stream") \
                            .withColumn("overall", col("overall").cast(DecimalType(10, 1))) \
                            .withColumn("style", from_json(col("style"), MapType(StringType(), StringType()))) \
                            .withColumn("unixReviewTime", from_unixtime(col("unixReviewTime").cast("long")).cast("date")) \
                            .withColumn("processing_time", current_timestamp()) \
                            .withColumn("processing_time", to_timestamp(col("processing_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
                            .drop(col("offset")) \
                            .drop(col("sequenceNumber")) \
                            .drop(col("enqueuedTime")) \
                        .writeStream \
                            .option("checkpointLocation", checkpoint_reviews_s_silver) \
                            .trigger(availableNow=True) \
                            .toTable(silver_s_reviews)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from silver.amazon_reviews_stream
