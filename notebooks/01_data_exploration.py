# Databricks notebook source
# MAGIC %md
# MAGIC #This is the EDA of the project

# COMMAND ----------

# MAGIC %fs ls mnt/azure-data-lake/amazon_reviews/partition_1/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`dbfs:/mnt/azure-data-lake/amazon_reviews/partition_1/part-00000-tid-9136122565017344171-3f98196e-e0c5-4bb5-90cc-d523170ef713-86080-1-c000.json.gz`

# COMMAND ----------

# MAGIC %md 
# MAGIC # hola
