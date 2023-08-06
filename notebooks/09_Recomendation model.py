# Databricks notebook source
# Read the data from the Gold schema
reviews_df = spark.table("gold.amazon_reviews_")

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import lower,col,regexp_replace,when,rand,explode,count,when,length,round
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
from pyspark.sql import DataFrame

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession with the desired configuration
spark = SparkSession.builder \
    .appName("recomendation") \
    .config("spark.driver.maxResultSize", "40g") \
    .getOrCreate()

# COMMAND ----------

df=reviews_df.filter((col('year') >2017))
df=df.select('asin','overall','reviewerID')
df=df.filter(col("overall")!=0.0)
df = df.dropna()

# COMMAND ----------

df.groupBy("overall").count().show()

# COMMAND ----------

# Step 1: Calculate the minimum count of any class in the "overall" column
minCount = df.groupBy("overall").count().agg(F.min("count")).collect()[0][0]

# Step 2: Calculate the sampling ratio for each class
majorityClass1 = df.filter(col("overall") == 5.0).sample(False, minCount / df.filter(col("overall") == 5.0).count())
majorityClass2 = df.filter(col("overall") == 4.0).sample(False, minCount / df.filter(col("overall") == 4.0).count())
majorityClass3 = df.filter(col("overall") == 1.0).sample(False, minCount / df.filter(col("overall") == 1.0).count())
majorityClass4 = df.filter(col("overall") == 3.0).sample(False, minCount / df.filter(col("overall") == 3.0).count())

# Step 3: Filter the minority class
minorityClass = df.filter(col("overall") == 2.0)


# COMMAND ----------

balancedData = majorityClass1.union(majorityClass2).union(majorityClass3).union(majorityClass4).union(minorityClass)
balancedData .groupBy("overall").count().show()

# COMMAND ----------

df=balancedData 

# COMMAND ----------

# MAGIC %md
# MAGIC Before making an ALS model it needs to be clear that ALS only accepts integer value as parameters. Hence we need to convert asin and reviewerID column in index form.

# COMMAND ----------

indexer = [StringIndexer(inputCol=column, outputCol=column+"_index",handleInvalid="skip") for column in list(set(df.columns)-set(['overall'])) ]
pipeline = Pipeline(stages=indexer)
transformed = pipeline.fit(df).transform(df)

# COMMAND ----------

transformed.show(2)

# COMMAND ----------

# Split the transformed DataFrame into training and test sets
(training, test) = transformed.randomSplit([0.8, 0.2])

# COMMAND ----------

als=ALS(maxIter=5,regParam=0.09,rank=25,userCol="reviewerID_index",itemCol="asin_index",ratingCol="overall",coldStartStrategy="drop",nonnegative=True)
model=als.fit(training)

# COMMAND ----------

import mlflow

with mlflow.start_run():
    mlflow.spark.log_model(model, "model_recomendation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate the prediction
# MAGIC

# COMMAND ----------

evaluator=RegressionEvaluator(metricName="rmse",labelCol="overall",predictionCol="prediction")
predictions=model.transform(test)
rmse=evaluator.evaluate(predictions)
print("RMSE="+str(rmse))
predictions.show()
