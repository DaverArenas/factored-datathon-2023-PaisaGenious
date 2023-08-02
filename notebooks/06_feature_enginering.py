# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime, to_date, split, from_utc_timestamp, from_json, date_format, to_timestamp, regexp_extract, when, lit
from pyspark.sql.types import MapType, StringType, DecimalType
import re
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.table("silver.amazon_metadata_test")
df = df.withColumnRenamed("asin", "asin_metadata")
keys = df.select("details.*").columns
for key in keys:
    df = df.withColumn(key.replace(".", "_"), col("details").getItem(key))

df = df.drop("Audio CD")

# COMMAND ----------

new_column_names = [col.replace(":", "").replace(",", "").replace(";", "").replace(" ", "_") for col in df.columns]
df = df.toDF(*new_column_names)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table silver.metadata_details

# COMMAND ----------

df.write \
.format("delta") \
.option("overwriteSchema", "true") \
.option("delta.columnMapping.mode", "name") \
.mode("overwrite") \
.saveAsTable("silver.metadata_details_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC select*
# MAGIC from silver.metadata_details_test
# MAGIC where Batteries is not null
# MAGIC limit 100
