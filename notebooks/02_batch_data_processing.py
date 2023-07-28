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
# MAGIC --Note: tables are automatically created during .writeStream.table("reviews") operation, but we can also use plainn SQL to create them
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
# MAGIC     vote                STRING,
# MAGIC     rescued_data        STRING
# MAGIC ) using delta tblproperties (
# MAGIC     delta.autooptimize.optimizewrite = TRUE,
# MAGIC     delta.autooptimize.autocompact = TRUE);
# MAGIC --With these 2 last options, Databricks engine will solve small files & optimize write

# COMMAND ----------

# MAGIC %sql
# MAGIC --Note: tables are automatically created during .writeStream.table("reviews") operation, but we can also use plainn SQL to create them
# MAGIC USE Bronze;
# MAGIC CREATE TABLE IF NOT EXISTS amazon_metadata (
# MAGIC     also_buy              STRING,
# MAGIC     also_view             STRING,
# MAGIC     asin                  STRING NOT NULL,
# MAGIC     brand                 STRING,
# MAGIC     category              STRING,
# MAGIC     date                  STRING,
# MAGIC     description           STRING,
# MAGIC     details               STRING,
# MAGIC     feature               STRING,
# MAGIC     fit                   STRING,
# MAGIC     image                 STRING,
# MAGIC     main_cat              STRING,
# MAGIC     price                 STRING,
# MAGIC     rank                  STRING,
# MAGIC     similar_item          STRING,
# MAGIC     tech1                 STRING,
# MAGIC     tech2                 STRING,
# MAGIC     title                 STRING
# MAGIC ) using delta tblproperties (
# MAGIC     delta.autooptimize.optimizewrite = TRUE,
# MAGIC     delta.autooptimize.autocompact = TRUE);

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
bronze_amazon_reviews = spark.readStream \
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
