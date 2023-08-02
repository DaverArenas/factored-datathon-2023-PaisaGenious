# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime, to_date, split, from_utc_timestamp, from_json, date_format, to_timestamp, regexp_extract, when, lit
from pyspark.sql.types import MapType, StringType, DecimalType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE silver;
# MAGIC CREATE TABLE IF NOT EXISTS amazon_reviews_test(
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
# MAGIC CREATE TABLE IF NOT EXISTS silver.amazon_metadata_test;
# MAGIC ALTER TABLE silver.amazon_metadata_test SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')

# COMMAND ----------

# Define checkpoint path for S3 bucket
s3_base_path = "s3://1-factored-datathon-2023-lakehouse"
silver_reviews = 'silver.amazon_reviews_test'
silver_metadata = 'silver.amazon_metadata_test'

checkpoint_reviews_silver = s3_base_path + '/Silver/amazon_reviews_test/'
checkpoint_metadata_silver = s3_base_path + '/Silver/amazon_metadata_test/'

# COMMAND ----------

# DBTITLE 1,Transformation for table amazon_reviews: cleaning and deduplicate
silver_amazon_reviews = spark.readStream \
                            .table("Bronze.amazon_reviews") \
                            .withColumn("overall", col("overall").cast(DecimalType(10, 1))) \
                            .withColumn("style", from_json(col("style"), MapType(StringType(), StringType()))) \
                            .withColumn("unixReviewTime", from_unixtime(col("unixReviewTime").cast("long")).cast("date")) \
                            .withColumn("processing_time", to_timestamp(col("processing_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
                            .dropDuplicates(subset=['reviewerID', 'asin', 'reviewText',"unixReviewTime","overall","summary"]) \
                            .drop(col("_rescued_data")) \
                            .drop(col("source_file")) \
                        .writeStream \
                            .option("checkpointLocation", checkpoint_reviews_silver) \
                            .trigger(availableNow=True) \
                            .toTable(silver_reviews)
silver_amazon_reviews.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Transformation for table amazon_metadata: cleaning and deduplicate
silver_amazon_metadata = spark.readStream \
                            .table("Bronze.amazon_metadata") \
                            .withColumn("main_cat", when(col("main_cat").like(''), lit('undefined')).otherwise(col("main_cat"))) \
                            .withColumn("extracted_category", 
                                        when(col("main_cat").like('<img src=%'), regexp_extract(col("main_cat"), 'alt="([^"]+)"', 1))
                                        .otherwise(col("main_cat"))) \
                            .drop(col("_rescued_data")) \
                            .drop(col("source_file")) \
                        .writeStream \
                            .option("checkpointLocation", checkpoint_metadata_silver) \
                            .option("mergeSchema", "true") \
                            .trigger(availableNow=True) \
                            .toTable(silver_metadata)
silver_amazon_metadata.awaitTermination()
