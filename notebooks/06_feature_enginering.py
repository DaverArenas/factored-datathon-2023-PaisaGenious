# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime, to_date, split, from_utc_timestamp, from_json, date_format, to_timestamp, regexp_extract, when, lit
from pyspark.sql.types import MapType, StringType, DecimalType
import re
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,queries to access subfields in struct types
# %sql
# select details
# from silver.amazon_metadata_clean
# where details.Batteries is not null
# limit 100
display(spark.table("silver.amazon_metadata_clean") \
        .where("details.Batteries is not null") \
        .limit(1)
)

# COMMAND ----------

df = spark.table("silver.amazon_metadata_clean")
df = df.withColumnRenamed("asin", "asin_metadata")
keys = df.select("details.*").columns
for key in keys:
    df = df.withColumn(key.replace(".", "_"), col("details").getItem(key))

df = df.drop("Audio CD")

display(df)

# COMMAND ----------

new_column_names = [col.replace(":", "").replace(",", "").replace(";", "").replace(" ", "_") for col in df.columns]
df = df.toDF(*new_column_names)

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table silver.metadata_details

# COMMAND ----------

df.write \
.format("delta") \
.option("overwriteSchema", "true") \
.option("delta.columnMapping.mode", "name") \
.saveAsTable("silver.metadata_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC select*
# MAGIC from silver.metadata_details
# MAGIC limit 100
