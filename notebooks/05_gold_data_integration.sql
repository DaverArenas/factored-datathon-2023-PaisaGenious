-- Databricks notebook source
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

select count(*)
from gold.amazon_reviews

-- COMMAND ----------

INSERT INTO gold.reviews_details
SELECT 
r.*, m.also_buy, m.also_view, m.brand, m.category, m.date, m.description, m.details, m.feature, m.main_cat, m.price, m.rank as rank_, m.similar_item, m.title
FROM gold.amazon_reviews AS r
LEFT JOIN silver.amazon_metadata AS m
ON r.asin = m.asin;

-- COMMAND ----------

select*
from gold.reviews_details
limit 10

-- COMMAND ----------

SELECT COUNT(DISTINCT reviewerID)
from gold.reviews_details

-- COMMAND ----------

COPY INTO gold.amazon_reviews
FROM silver.reviews_streaming
FILEFORMAT = <format>
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

select*
from silver.amazon_reviews
limit 10

-- COMMAND ----------

select*
from silver.amazon_metadata

-- COMMAND ----------

-- Assuming you have a table named 'your_table_name'
DESCRIBE silver.amazon_metadata;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT r.*, m.also_buy, m.also_view, m.brand, m.category, m.date, m.description, m.details, m.feature, m.main_cat, m.price, m.rank, m.similar_item, m.title
-- MAGIC FROM silver.amazon_reviews AS r
-- MAGIC INNER JOIN silver.amazon_metadata AS m
-- MAGIC ON r.asin = m.asin
-- MAGIC LIMIT 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS silver.reviews_metadata

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC ALTER TABLE silver.reviews_metadata SET TBLPROPERTIES (
-- MAGIC    'delta.columnMapping.mode' = 'name',
-- MAGIC    'delta.minReaderVersion' = '2',
-- MAGIC    'delta.minWriterVersion' = '5')

-- COMMAND ----------

# Assuming you have a SparkSession named 'spark'
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- CREATE TABLE silver.reviews_metadata;
-- MAGIC -- -- Enable column mapping
-- MAGIC -- ALTER TABLE silver.reviews_metadata SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
-- MAGIC
-- MAGIC -- Insert the data into the existing table
-- MAGIC INSERT INTO silver.reviews_metadata
-- MAGIC SELECT r.*, m.also_buy, m.also_view, m.brand, m.category, m.date, m.description, m.details, m.feature, m.main_cat, m.price, m.rank as rank_, m.similar_item, m.title
-- MAGIC FROM silver.amazon_reviews AS r
-- MAGIC INNER JOIN silver.amazon_metadata AS m
-- MAGIC ON r.asin = m.asin;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select*
-- MAGIC from silver.reviews_metadata
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) 
-- MAGIC from silver.amazon_metadata

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select extracted_category, count(*) as total
-- MAGIC from silver.amazon_metadata_clean
-- MAGIC group by extracted_category
-- MAGIC order by total desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select *
-- MAGIC from silver.amazon_metadata_clean
-- MAGIC where main_cat = ''
-- MAGIC -- group by extracted_category
-- MAGIC -- order by total desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   main_cat,
-- MAGIC   CASE WHEN main_cat LIKE '<img src=%' THEN regexp_extract(main_cat, 'alt="([^"]+)"', 1)
-- MAGIC     ELSE main_cat
-- MAGIC   END AS extracted_category
-- MAGIC FROM silver.reviews_metadata
-- MAGIC LIMIT 100

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_reviews_metadata_clean AS
-- MAGIC SELECT *,
-- MAGIC       CASE WHEN main_cat LIKE '<img src=%' THEN regexp_extract(main_cat, 'alt="([^"]+)"', 1) ELSE main_cat
-- MAGIC       END AS extracted_category
-- MAGIC FROM silver.reviews_metadata

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select extracted_category, count(*) as total
-- MAGIC from temp_reviews_metadata_clean
-- MAGIC group by extracted_category
-- MAGIC order by total desc
