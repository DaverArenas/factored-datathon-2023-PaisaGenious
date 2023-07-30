# Databricks notebook source
# MAGIC %md
# MAGIC ## 1/ Bronze: Loading data from azure datalake storage account
# MAGIC We'll store the raw data in Bronze.amazon_reviews and Bronze.amazon_metadata DELTA table, supporting schema evolution and incorrect data

# COMMAND ----------

# Import functions
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating schema Bronze for the first time
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Note: tables are automatically created during .writeStream.table("reviews") operation, but we can also use plain SQL to create them
# MAGIC USE Bronze;
# MAGIC CREATE TABLE IF NOT EXISTS amazon_reviews(
# MAGIC     asin                STRING NOT NULL,
# MAGIC     image               STRING,
# MAGIC     overall             STRING,
# MAGIC     reviewText          STRING,
# MAGIC     reviewerID          STRING,
# MAGIC     reviewerName        STRING,
# MAGIC     style               STRING,
# MAGIC     summary             STRING,
# MAGIC     unixReviewTime      STRING,
# MAGIC     verified            STRING,
# MAGIC     vote                STRING
# MAGIC ) using delta tblproperties (
# MAGIC     delta.autooptimize.optimizewrite = TRUE,
# MAGIC     delta.autooptimize.autocompact = TRUE);
# MAGIC --With these 2 last options, Databricks engine will solve small files & optimize write

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.amazon_metadata;
# MAGIC ALTER TABLE bronze.amazon_metadata SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')

# COMMAND ----------

# %sql
# --Note: tables are automatically created during .writeStream.table("reviews") operation, but we can also use plain SQL to create them
# USE Bronze;
# CREATE TABLE IF NOT EXISTS amazon_metadata (
#     also_buy              ARRAY<STRING>,
#     also_view             ARRAY<STRING>,
#     asin                  STRING NOT NULL,
#     brand                 STRING,
#     category              ARRAY<STRING>,
#     date                  STRING,
#     description           ARRAY<STRING>,
#     details               STRING,
#     feature               STRING,
#     fit                   STRING,
#     image                 STRING,
#     main_cat              STRING,
#     price                 STRING,
#     rank                  STRING,
#     similar_item          STRING,
#     tech1                 STRING,
#     tech2                 STRING,
#     title                 STRING
# ) using delta tblproperties (
#     delta.autooptimize.optimizewrite = TRUE,
#     delta.autooptimize.autocompact = TRUE);

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simple, resilient & scalable data loading with Databricks Autoloader

# COMMAND ----------

# Define the base path for S3 bucket
s3_base_path = "s3://1-factored-datathon-2023-lakehouse"
bronze_reviews = 'amazon_reviews'
bronze_metadata = 'amazon_metadata'
checkpoint_reviews = s3_base_path + '/Bronze/amazon_reviews/'
checkpoint_metadata = s3_base_path + '/Bronze/amazon_metadata/'

# COMMAND ----------

# DBTITLE 1,Stream api with Autoloader allows batch incremental data loading
bronze_amazon_reviews = spark.readStream \
                            .format("cloudFiles") \
                            .option("cloudFiles.format", "json") \
                            .option("cloudFiles.schemaLocation", s3_base_path+"/schemas/amazon_reviews/") \
                            .option("cloudFiles.inferColumnTypes", True) \
                            .load("dbfs:/mnt/azure-data-lake/amazon_reviews/*/*.json") \
                            .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time")) \
                            .writeStream \
                            .option("checkpointLocation", checkpoint_reviews) \
                            .option("mergeSchema", "true") \
                            .trigger(availableNow=True) \
                            .toTable(bronze_reviews)

# COMMAND ----------

# DBTITLE 1,Batch Incremental data loading amazon_metadata
bronze_amazon_metadata = spark.readStream \
                            .format("cloudFiles") \
                            .option("cloudFiles.format", "json") \
                            .option("cloudFiles.schemaLocation", s3_base_path+"/schemas/amazon_metadata/") \
                            .option("cloudFiles.inferColumnTypes", True) \
                            .load("dbfs:/mnt/azure-data-lake/amazon_metadata/*/*.json") \
                            .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time")) \
                            .writeStream \
                            .option("checkpointLocation", checkpoint_metadata) \
                            .option("mergeSchema", "true") \
                            .trigger(availableNow=True) \
                            .toTable(bronze_metadata)
