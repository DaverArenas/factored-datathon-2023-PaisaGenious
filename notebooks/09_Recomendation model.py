# Databricks notebook source
df_reviews = spark.read.format("delta").load("s3://1-factored-datathon-2023-lakehouse/gold/review_cleaned2")

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import lower,col,regexp_replace,when,rand,explode
from pyspark.sql import functions as F

from pyspark.ml.feature import HashingTF, IDF,StringIndexer,VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import string
import re
#import mlflow

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline

# COMMAND ----------

df=df_reviews.select('asin','overall','reviewerID',"verified")
#df.show(10)

# COMMAND ----------

total_rows= df.count()
print("Number of rows in the DataFrame of amazon reviews: ", total_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC Before making an ALS model it needs to be clear that ALS only accepts integer value as parameters. Hence we need to convert asin and reviewerID column in index form.

# COMMAND ----------

indexer = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in list(set(df.columns)-set(['overall'])) ]
pipeline = Pipeline(stages=indexer)
transformed = pipeline.fit(df).transform(df)
transformed.show()

# COMMAND ----------

import mlflow
logged_model = 'runs:/15082a6cf007425797b1efa40192f2cf/best_model'

# Load model
loaded_model = mlflow.spark.load_model(logged_model)

# Perform inference via model.transform()
#loaded_model.transform(data)

# COMMAND ----------

data=spark.table("silver.reviews_streaming")

# COMMAND ----------

data=data.select("reviewText")

# COMMAND ----------

loaded_model.transform(data)

# COMMAND ----------

predict=loaded_model.transform(data)

# COMMAND ----------

predict.select("prediction").show()
