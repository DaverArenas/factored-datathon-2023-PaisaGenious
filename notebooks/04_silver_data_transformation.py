# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime, to_date, split, from_utc_timestamp, from_json, date_format, to_timestamp
from pyspark.sql.types import MapType, StringType, DecimalType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE silver;
# MAGIC CREATE TABLE IF NOT EXISTS amazon_reviews(
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

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.amazon_metadata;
# MAGIC ALTER TABLE silver.amazon_metadata SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE silver;
# MAGIC CREATE TABLE IF NOT EXISTS amazon_metadata (
# MAGIC     also_buy            ARRAY<STRING>,
# MAGIC     also_view           ARRAY<STRING>,
# MAGIC     asin                STRING,
# MAGIC     brand               STRING,
# MAGIC     category            ARRAY<STRING>,
# MAGIC     date                STRING,
# MAGIC     description         ARRAY<STRING>,
# MAGIC     details             STRUCT<>,
# MAGIC     feature             ARRAY<STRING>,
# MAGIC     fit                 STRING,
# MAGIC     image               ARRAY<STRING>,
# MAGIC     main_cat            STRING,
# MAGIC     price               STRING,
# MAGIC     rank                ARRAY<STRING>,
# MAGIC     similar_item        STRING,
# MAGIC     tech1               STRING,
# MAGIC     tech2               STRING,
# MAGIC     title               STRING,
# MAGIC     processing_time     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# Define the base path for S3 bucket
s3_base_path = "s3://1-factored-datathon-2023-lakehouse"
silver_reviews = 'silver.amazon_reviews'
silver_metadata = 'silver.amazon_metadata'

checkpoint_reviews_silver = s3_base_path + '/Silver/amazon_reviews/'
checkpoint_metadata_silver = s3_base_path + '/Silver/amazon_metadata/'

# COMMAND ----------

silver_amazon_reviews = spark.readStream \
                            .table("Bronze.amazon_reviews") \
                            .withColumn("overall", col("overall").cast(DecimalType(10, 1))) \
                            .withColumn("style", from_json(col("style"), MapType(StringType(), StringType()))) \
                            .withColumn("unixReviewTime", from_unixtime(col("unixReviewTime").cast("long")).cast("date")) \
                            .withColumn("processing_time", to_timestamp(col("processing_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
                            .drop(col("_rescued_data")) \
                            .drop(col("source_file")) \
                        .writeStream \
                            .option("checkpointLocation", checkpoint_reviews_silver) \
                            .trigger(availableNow=True) \
                            .toTable(silver_reviews)

# COMMAND ----------

# DBTITLE 1,Silver transformation for table amazon_metadata
silver_amazon_metadata = spark.readStream \
                            .table("Bronze.amazon_metadata") \
                            .drop(col("_rescued_data")) \
                            .drop(col("source_file")) \
                        .writeStream \
                            .option("checkpointLocation", checkpoint_metadata_silver) \
                            .option("mergeSchema", "true") \
                            .trigger(availableNow=True) \
                            .toTable(silver_metadata)
