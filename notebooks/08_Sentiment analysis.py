# Databricks notebook source
# MAGIC %md
# MAGIC ## Performing Sentiment Analysis on Amazon Reviews 

# COMMAND ----------

!pip install nltk

# COMMAND ----------

!pip install textBlob

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import lower,col,regexp_replace,when,rand
from pyspark.sql import functions as F

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import string
import re
#import wordcloud
from nltk.corpus import stopwords
import nltk
from pyspark.ml.feature import StopWordsRemover
import mlflow
#from textblob import TextBlob

from pyspark.sql.functions import udf, col, size
from pyspark.sql.types import ArrayType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data loading

# COMMAND ----------

# Read the data from the Gold schema
reviews_df = spark.table("silver.amazon_reviews")

# COMMAND ----------

df_reviews = spark.read.format("delta").load("s3://1-factored-datathon-2023-lakehouse/Bronze/review_cleaned")

# COMMAND ----------

# Define a UDF to calculate the polarity of each review
def get_sentiment(review):
    if review is not None and isinstance(review, str):
        return TextBlob(review).sentiment.polarity
    else:
        return 0.0  # Assign a neutral polarity for missing or non-string reviews


# COMMAND ----------

# Add a new column "sentiment_label" based on sentiment polarity
reviews_df = df_reviews.withColumn("label", 
                                   when(col("overall") < 3.0, 0)
                                   .when(col("overall") == 3.0, 0)
                                   .when(col("overall") > 3.0, 1))

# COMMAND ----------

reviews_df.groupBy('label').count().show()

# COMMAND ----------

reviews_df=reviews_df.select("reviewText","label")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data processsing

# COMMAND ----------

# Define preprocessing function
def clean(text):
    import html
    import string
    import nltk
    nltk.download('wordnet')
    nltk.download('omw-1.4')

    line = html.unescape(text)
    line = line.replace("can't", 'can not')
    line = line.replace("n't", " not")
    # Pad punctuations with white spaces
    pad_punct = str.maketrans({key: " {0} ".format(key) for key in string.punctuation}) 
    line = line.translate(pad_punct)
    line = line.lower()
    line = line.split() 
    lemmatizer = nltk.WordNetLemmatizer()
    line = [lemmatizer.lemmatize(t) for t in line] 

    # Negation handling
    # Add "not_" prefix to words behind "not", or "no" until the end of the sentence
    tokens = []
    negated = False
    for t in line:
        if t in ['not', 'no']:
            negated = not negated
        elif t in string.punctuation or not t.isalpha():
            negated = False
        else:
            tokens.append('not_' + t if negated else t)
    
    invalidChars = str(string.punctuation.replace("_", ""))  
    bi_tokens = list(nltk.bigrams(line))
    bi_tokens = list(map('_'.join, bi_tokens))
    bi_tokens = [i for i in bi_tokens if all(j not in invalidChars for j in i)]
    tri_tokens = list(nltk.trigrams(line))
    tri_tokens = list(map('_'.join, tri_tokens))
    tri_tokens = [i for i in tri_tokens if all(j not in invalidChars for j in i)]
    tokens = tokens + bi_tokens + tri_tokens      
    
    return tokens

# COMMAND ----------

# An example: how the function clean() pre-processes the input text
example = clean("I don't think this book has any decent information!!! It is full of typos and factual errors that I can't ignore.")
print(example)

# COMMAND ----------

# Perform data preprocessing
clean_udf = udf(clean, ArrayType(StringType()))
data_tokens = reviews_df.withColumn('tokens', clean_udf(col('reviewText')))
#data=data_tokens.select("reviewText","tokens","label")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Class balancing

# COMMAND ----------

# Get the counts of each class
class_counts =data_tokens.groupBy("label").count()

# Calculate the smallest class count (minimum count)
min_class_count = class_counts.agg({"count": "min"}).collect()[0][0]

# Randomly undersample the majority class and medium class
undersampled_df = data_tokens.filter(col("label") != 1)  # Keep all non-majority class samples
majority_class_df = data_tokens.filter(col("label") == 1)  # Get all majority class samples

# Undersample the majority class
undersampled_majority_class_df = majority_class_df.sample(False, min_class_count / majority_class_df.count())

# Union the undersampled dataframes to create the balanced dataframe
balanced_df = undersampled_df.union(undersampled_majority_class_df)

# COMMAND ----------

wordsDF=balanced_df.select("reviewText","label","tokens")
df=balanced_df.select("reviewText","label")
#wordsDF.groupBy("label").count().show()

# COMMAND ----------

# Tokenize the review. 
tokenizer = Tokenizer(inputCol="reviewText", outputCol="review_words")
wordsDFp = tokenizer.transform(df)
wordsDF#.show(2)

# COMMAND ----------

# Remove stop words
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
wordsDF2 = remover.transform(wordsDF)
#wordsDF2.show(10)

# COMMAND ----------

# Convert to TF words vector
hashingTF = HashingTF(inputCol="filtered", outputCol="TF")
wordsDF3 = hashingTF.transform(wordsDF2)
wordsDF3.show(2)

# COMMAND ----------

# Convert to TF words vector
hashingTF = HashingTF(inputCol="filtered", outputCol="TF")
wordsDF3 = hashingTF.transform(wordsDF2)
wordsDF3.show(2)

# COMMAND ----------

## HashingTF in SparkML cannot normalize term frequency with the total number of words in each document
for features_label in wordsDF3.select("TF", "label").take(1):
    print(features_label)

# COMMAND ----------

# Convert to IDF words vector, ensure to name the features as 'features'
idf = IDF(inputCol="TF", outputCol="features")
idfModel = idf.fit(wordsDF3)
#wordsDF4 = idfModel.transform(wordsDF3)
#wordsDF4.show(10)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Model training

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline

# Split data into training and testing set 
(training, test) = df.randomSplit([0.7, 0.3])

# Create a logistic regression instance
lr = LogisticRegression(maxIter=10)

# Use a pipeline to chain all transformers and estimators
pipeline = Pipeline(stages=[remover, hashingTF, idfModel, lr])

# COMMAND ----------

# We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
# This will allow us to jointly choose parameters for all Pipeline stages.
# A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
# We use a ParamGridBuilder to construct a grid of parameters to search over.
paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 50]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

# COMMAND ----------


crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=3) 

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(training)

# Make predictions on test documents. cvModel uses the best model found (lrModel).
prediction = cvModel.transform(test)


# COMMAND ----------

selected = prediction.select("reviewText", "label", "probability", "prediction").take(5)
for row in selected:
    print(row)

# Evaluate result with ROC
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", metricName="accuracy")
accuaracy= evaluator.evaluate(prediction)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving the Models

# COMMAND ----------

with mlflow.start_run():
    mlflow.spark.log_model(cvModel, "model_sentiment_analysis")

# COMMAND ----------

logged_model = 'runs:/15082a6cf007425797b1efa40192f2cf/best_model'

# Load model
loaded_model = mlflow.spark.load_model(logged_model)

