# Databricks notebook source
# MAGIC %md
# MAGIC ## Performing Sentiment Analysis on Amazon Reviews 

# COMMAND ----------

!pip install nltk

# COMMAND ----------

!pip install textBlob

# COMMAND ----------

!pip install wordcloud

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from textblob import TextBlob
from pyspark.sql.functions import lower,col,regexp_replace,when,rand
from pyspark.sql import functions as F

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import string
import re
import wordcloud
from nltk.corpus import stopwords
import nltk


# COMMAND ----------

# MAGIC %md
# MAGIC #### Data loading

# COMMAND ----------

# Read the data from the Gold schema
reviews_df = spark.table("silver.amazon_reviews")

# COMMAND ----------

df_reviews = spark.read.format("delta").load("s3://1-factored-datathon-2023-lakehouse/Bronze/final_review")

# COMMAND ----------

# Define a UDF to calculate the polarity of each review
def get_sentiment(review):
    if review is not None and isinstance(review, str):
        return TextBlob(review).sentiment.polarity
    else:
        return 0.0  # Assign a neutral polarity for missing or non-string reviews


# COMMAND ----------

# Add a new column "sentiment_label" based on sentiment polarity
reviews_df = df_reviews.withColumn("sentiment_label", 
                                   when(col("overall") < 3.0, -1)
                                   .when(col("overall") == 3.0, 0)
                                   .when(col("overall") > 3.0, 1))

# COMMAND ----------

reviews_df.groupBy('sentiment_label').count().show()

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
from pyspark.sql.functions import udf, col, size
from pyspark.sql.types import ArrayType, StringType

clean_udf = udf(clean, ArrayType(StringType()))
data_tokens = reviews_df.withColumn('tokens', clean_udf(col('reviewText')))
#data_tokens.select("reviewText","tokens").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Class balancing

# COMMAND ----------

# Get the counts of each class
class_counts = data_tokens.groupBy("sentiment_label").count()
# Calculate the smallest class count (minimum count)
min_class_count = class_counts.agg(F.min("count")).collect()[0]["min(count)"]

# Randomly undersample the majority class
undersampled_df = data_tokens.filter(col("sentiment_label") == 0)  # Keep all non-majority class samples
majority_class_df = data_tokens.filter(col("sentiment_label") == 1)  # Get all majority class samples
medium_class_df = data_tokens.filter(col("sentiment_label") == -1)  # Get all majority class samples
undersampled_majority_class_df = majority_class_df.orderBy(rand()).limit(min_class_count)
undersampled_medium_class_df = medium_class_df.orderBy(rand()).limit(min_class_count)
balanced_df = undersampled_df.union(undersampled_majority_class_df)
balanced_df = balanced_df.union(undersampled_medium_class_df)

# COMMAND ----------

balanced_df.groupBy("sentiment_label").count().show()

# COMMAND ----------

final_df=balanced_df.select("reviewText","tokens","sentiment_label")
#final_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Split the dataset

# COMMAND ----------

# Split data to 70% for training and 30% for testing
training, testing = final_df.randomSplit([0.7,0.3], seed=1220)
#training.groupBy('sentiment_label').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Models

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Naive Bayes Model (with parameter tuning)

# COMMAND ----------


from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml import Pipeline

count_vec = CountVectorizer(inputCol='tokens', outputCol='c_vec', minDF=5.0)
idf = IDF(inputCol="c_vec", outputCol="features")

# COMMAND ----------


# Naive Bayes model
from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes()

pipeline_nb = Pipeline(stages=[count_vec, idf, nb])

model_nb = pipeline_nb.fit(training)
test_nb = model_nb.transform(testing)
#test_nb.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Logistic Regressions

# COMMAND ----------


# Logistic Regression model
from pyspark.ml.classification import LogisticRegression

lgr = LogisticRegression(maxIter=5)
pipeline_lgr = Pipeline(stages=[count_vec, idf, lgr])

model_lgr = pipeline_lgr.fit(training)
test_lgr = model_lgr.transform(testing)

# COMMAND ----------


# Logistic Regression model ROC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
roc_lgr_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='label')
roc_lgr = roc_lgr_eval.evaluate(test_lgr)
print("ROC of the model: {}".format(roc_lgr))

# COMMAND ----------


# Logistic Regression model accuracy
#from pyspark.ml.evaluation import MulticlassClassificationEvaluator
acc_lgr_eval = MulticlassClassificationEvaluator(metricName='accuracy')
acc_lgr = acc_lgr_eval.evaluate(test_lgr)
print("Accuracy of the model: {}".format(acc_lgr))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Predict on new reviews
