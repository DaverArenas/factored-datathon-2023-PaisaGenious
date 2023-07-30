# Databricks notebook source
import base64
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT*
# MAGIC FROM bronze.amazon_streaming
# MAGIC LIMIT 10

# COMMAND ----------

def decode_and_transform_udf(data):
    decoded_data = base64.b64decode(data).decode('utf-8')
    return json.loads(decoded_data)

# COMMAND ----------

# Register the UDF with Spark
decode_and_transform = udf(decode_and_transform_udf, StringType())

# COMMAND ----------

silver_amazon_streams = spark.readStream \
    .table("bronze.amazon_streaming") \
    .withColumn("newColumn", decode_and_transform("body"))

# COMMAND ----------

display(silver_amazon_streams)

# COMMAND ----------

silver_amazon_streams = spark.readStream \
                            .table("bronze.amazon_streaming") \
                            .withColumn("newColumn", new code in here) \
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

import base64
import json

data_encoded = "eyJhc2luIjoiQjAxNjE0U1VZRyIsIm92ZXJhbGwiOiI1LjAiLCJyZXZpZXdUZXh0IjoidGhpcyByZWFseSBncm93cyBmYXN0ICAuIEkgY2FudCB3YWl0IGZvciBteSB0b21hdG9zIHRvIGdyb3cgLiBvdXQgSSBhbSBlbmpveWk="

# Decode the Base64 encoded data
decoded_data = base64.b64decode(data_encoded).decode('utf-8')

# Print the decoded data
print(decoded_data)
