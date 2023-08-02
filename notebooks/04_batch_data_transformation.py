# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime, to_date, split, from_utc_timestamp, from_json, date_format, to_timestamp, regexp_extract, when, lit
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
# MAGIC CREATE TABLE IF NOT EXISTS silver.amazon_metadata_clean;
# MAGIC ALTER TABLE silver.amazon_metadata_clean SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')

# COMMAND ----------

# Define checkpoint path for S3 bucket
s3_base_path = "s3://1-factored-datathon-2023-lakehouse"
silver_reviews = 'silver.amazon_reviews'
silver_metadata = 'silver.amazon_metadata_clean'

checkpoint_reviews_silver = s3_base_path + '/Silver/amazon_reviews/'
checkpoint_metadata_silver = s3_base_path + '/Silver/amazon_metadata_clean/'

# COMMAND ----------

# DBTITLE 1,Transformation for table amazon_reviews: cleaning and deduplicate
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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), count(asin), count(image), count(overall), count(reviewText), count(reviewerID), count(reviewerName)
# MAGIC FROM bronze.amazon_reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.*, m.also_buy, m.also_view, m.brand, m.category, m.date, m.description, m.details, m.feature, m.main_cat, m.price, m.rank, m.similar_item, m.title
# MAGIC FROM silver.amazon_reviews AS r
# MAGIC INNER JOIN silver.amazon_metadata AS m
# MAGIC ON r.asin = m.asin
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.reviews_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.reviews_metadata SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')

# COMMAND ----------

# Assuming you have a SparkSession named 'spark'
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE silver.reviews_metadata;
# MAGIC -- -- Enable column mapping
# MAGIC -- ALTER TABLE silver.reviews_metadata SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC -- Insert the data into the existing table
# MAGIC INSERT INTO silver.reviews_metadata
# MAGIC SELECT r.*, m.also_buy, m.also_view, m.brand, m.category, m.date, m.description, m.details, m.feature, m.main_cat, m.price, m.rank as rank_, m.similar_item, m.title
# MAGIC FROM silver.amazon_reviews AS r
# MAGIC INNER JOIN silver.amazon_metadata AS m
# MAGIC ON r.asin = m.asin;

# COMMAND ----------

# MAGIC %sql
# MAGIC select*
# MAGIC from silver.reviews_metadata
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from silver.amazon_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC select extracted_category, count(*) as total
# MAGIC from silver.amazon_metadata_clean
# MAGIC group by extracted_category
# MAGIC order by total desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from silver.amazon_metadata_clean
# MAGIC where main_cat = ''
# MAGIC -- group by extracted_category
# MAGIC -- order by total desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   main_cat,
# MAGIC   CASE WHEN main_cat LIKE '<img src=%' THEN regexp_extract(main_cat, 'alt="([^"]+)"', 1)
# MAGIC     ELSE main_cat
# MAGIC   END AS extracted_category
# MAGIC FROM silver.reviews_metadata
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_reviews_metadata_clean AS
# MAGIC SELECT *,
# MAGIC       CASE WHEN main_cat LIKE '<img src=%' THEN regexp_extract(main_cat, 'alt="([^"]+)"', 1) ELSE main_cat
# MAGIC       END AS extracted_category
# MAGIC FROM silver.reviews_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC select extracted_category, count(*) as total
# MAGIC from temp_reviews_metadata_clean
# MAGIC group by extracted_category
# MAGIC order by total desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.amazon_metadata
# MAGIC WHERE main_cat LIKE '<img src=%'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE silver.reviews_metadata;
# MAGIC -- -- Enable column mapping
# MAGIC -- ALTER TABLE silver.reviews_metadata SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC -- Insert the data into the existing table
# MAGIC INSERT INTO silver.reviews_metadata
# MAGIC SELECT r.*, m.also_buy, m.also_view, m.brand, m.category, m.date, m.description, m.details, m.feature, m.main_cat, m.price, m.rank as rank_, m.similar_item, m.title
# MAGIC FROM silver.amazon_reviews AS r
# MAGIC INNER JOIN silver.amazon_metadata AS m
# MAGIC ON r.asin = m.asin;
