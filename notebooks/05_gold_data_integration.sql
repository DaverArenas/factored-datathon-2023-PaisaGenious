-- Databricks notebook source
select*
from silver.amazon_reviews
limit 10

-- COMMAND ----------

select*
from silver.amazon_metadata

-- COMMAND ----------

-- Assuming you have a table named 'your_table_name'
DESCRIBE silver.amazon_metadata;
