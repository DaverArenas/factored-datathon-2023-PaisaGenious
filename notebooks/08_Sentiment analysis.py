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
from pyspark.sql.functions import lower,col,regexp_replace,when,rand,year,from_unixtime,unix_timestamp,length
from pyspark.sql import functions as F
from html import unescape

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
from textblob import TextBlob

from pyspark.sql.functions import udf, col, size
from pyspark.sql.types import ArrayType, StringType,FloatType,DoubleType

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data loading

# COMMAND ----------

# Read the data from the Gold schema
reviews_df = spark.table("gold.amazon_reviews_")

# COMMAND ----------

reviews_df=reviews_df.select("reviewText","Year","overall")
reviews_df=reviews_df.filter(col("Year")>2016)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data processsing

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing special characters

# COMMAND ----------

# Function to remove non-word characters using regex
def remove_special_characters(text):
    return re.sub(r'[^\w\s]', '', text)

# Register UDF
remove_special_characters_udf = udf(remove_special_characters, StringType())

# Apply UDF to remove non-word characters from the 'Text' column
df_cleaned_review = reviews_df.withColumn('cleanedReviewText', remove_special_characters_udf(col('reviewText')))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing html tags

# COMMAND ----------

# Function to remove HTML tags using regex
def remove_html(text):
    cleanr = re.compile('<.*?>')
    cleanr2 = re.compile('<.*?</a>')
    textm=re.sub(cleanr2, '', text)
    textf=re.sub(cleanr, '', textm)
    return textf

# Register UDF
remove_html_udf = udf(remove_html, StringType())

# Apply UDF to remove HTML tags from the 'Text' column
df_cleaned_review= df_cleaned_review.withColumn("cleanedReviewText", remove_html_udf(col("cleanedReviewText")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lowercase
# MAGIC

# COMMAND ----------

# Function to convert text to lowercase and join
def lowercase_and_join(text):
    return " ".join(word.lower() for word in text.split())

# Register UDF
lowercase_and_join_udf = udf(lowercase_and_join, StringType())

# Apply UDF to convert the text to lowercase and join it back
df_cleaned_review= df_cleaned_review.withColumn('cleanedReviewText', lowercase_and_join_udf(col('cleanedReviewText')))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing URLs

# COMMAND ----------

# Function to remove URLs using regex
def remove_url(text):
    url = re.compile(r'https?://\S+|www\.\S+')
    return url.sub(r'', text)

# Register UDF
remove_url_udf = udf(remove_url, StringType())

# Apply UDF to remove URLs from the 'Text' column
df_cleaned_review = df_cleaned_review.withColumn("cleanedReviewText", remove_url_udf(col("cleanedReviewText")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing emojis

# COMMAND ----------

# Function to remove emojis using regex
def remove_emoji(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    
    return emoji_pattern.sub(r'', text)

# Register UDF
remove_emoji_udf = udf(remove_emoji, StringType())

# Apply UDF to remove emojis from the 'Text' column
df_cleaned_review = df_cleaned_review.withColumn("cleanedReviewText", remove_emoji_udf(col("cleanedReviewText")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing emoticons

# COMMAND ----------

def remove_emoticons(text):
    emoticon_pattern = re.compile(r'(?::|;|=)(?:-)?(?:\)|\(|D|P)')
    return emoticon_pattern.sub(r'', text)

# Register UDF
remove_emoticons_udf = udf(remove_emoticons, StringType())

# Apply UDF to remove emoticons from the 'Text' column
df_cleaned_review= df_cleaned_review.withColumn("cleanedReviewText", remove_emoticons_udf(col("cleanedReviewText")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing numerical characters

# COMMAND ----------

# Use regexp_replace to remove numerical characters from the 'text_column'
df_cleaned_review= df_cleaned_review.withColumn("cleanedReviewText",regexp_replace(col("cleanedReviewText"), r'\d+', ''))
# Remove rows with cleanedReviewText as "" or with less than 4 characters
df_cleaned_review = df_cleaned_review.filter((length(col("cleanedReviewText")) >= 4) & (col("cleanedReviewText") != ""))
#df=df_cleaned_review.select("cleanedReviewText","label")
df=df_cleaned_review.select("cleanedReviewText","overall")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Labeling the data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Approach 1 - Based on "Overall":

# COMMAND ----------

# Add a new column "sentiment_label" based on sentiment polarity
df = df_cleaned_review.withColumn("label1", when(col("overall") < 3.0, 0)
                                   .when(col("overall") == 3.0, 0)
                                   .when(col("overall") > 3.0, 1))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Approach 2 - Polarity Calculation":

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calculating polarity

# COMMAND ----------

# Define a UDF to calculate the polarity of each review
def get_sentiment(review):
    if review is not None and isinstance(review, str):
        return TextBlob(review).sentiment.polarity
    else:
        return 0.0  # Assign a neutral polarity for missing or non-string reviews
    
# Register the UDF with Spark
sentiment_udf = udf(get_sentiment, FloatType())

# Assume 'df' is your Spark DataFrame with a column named 'review' containing the text reviews
# Add a new column 'sentiment' with the calculated sentiment using the UDF
df = df.withColumn("polarity", sentiment_udf(df["cleanedReviewText"]))

# COMMAND ----------

# Add a new column "sentiment_label" based on sentiment polarity
df = df.withColumn("label", when((col("polarity") < 0.0) | (col("polarity") == 0.0), 0).when(col("polarity") > 0.0, 1))


# COMMAND ----------

df1=df.select("label1","cleanedReviewText")
display(df1.groupBy("label1").count())

# COMMAND ----------

df2=df.select("label","cleanedReviewText")
display(df.groupBy("label").count())

# COMMAND ----------

df=df2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Class balancing

# COMMAND ----------

# Get the counts of each class
class_counts =df.groupBy("label").count()

# Calculate the smallest class count (minimum count)
min_class_count = class_counts.agg({"count": "min"}).collect()[0][0]

# Randomly undersample the majority class and medium class
undersampled_df = df.filter(col("label") != 1)  # Keep all non-majority class samples
majority_class_df = df.filter(col("label") == 1)  # Get all majority class samples

# Undersample the majority class
undersampled_majority_class_df = majority_class_df.sample(False, min_class_count / majority_class_df.count())

# Union the undersampled dataframes to create the balanced dataframe
balanced_df = undersampled_df.union(undersampled_majority_class_df)

# COMMAND ----------

df=balanced_df
df.groupBy("label").count().show()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature extraction: Term Frequency (TF) Vectors:

# COMMAND ----------

# Tokenize the review. 
tokenizer = Tokenizer(inputCol="cleanedReviewText", outputCol="review_words")
wordsDF = tokenizer.transform(df)
#wordsDF.show(2)

# COMMAND ----------

# Remove stop words
remover = StopWordsRemover(inputCol="review_words", outputCol="filtered")
wordsDF2 = remover.transform(wordsDF)
#wordsDF2.show(10)

# COMMAND ----------

# Convert to TF words vector
hashingTF = HashingTF(inputCol="filtered", outputCol="TF")
wordsDF3 = hashingTF.transform(wordsDF2)
#wordsDF3.show(2)

# COMMAND ----------

## HashingTF in SparkML cannot normalize term frequency with the total number of words in each document
for features_label in wordsDF3.select("TF", "label").take(3):
    print(features_label)

# COMMAND ----------

# Convert to IDF words vector, ensure to name the features as 'features'
idf = IDF(inputCol="TF", outputCol="features")
idfModel = idf.fit(wordsDF3)
wordsDF4 = idfModel.transform(wordsDF3)
#wordsDF4.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model training

# COMMAND ----------

# Split data into training and testing set 
(training, test) = df.randomSplit([0.8, 0.2])

# Create a logistic regression instance
lr = LogisticRegression()

# Use a pipeline to chain all transformers and estimators
pipeline = Pipeline(stages=[tokenizer,remover, hashingTF, idfModel, lr])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Grid search and cross validation

# COMMAND ----------

# Create ParamGrid for Cross Validation 
paramGrid2 = (ParamGridBuilder()
             .addGrid(hashingTF.numFeatures, [50000])
             .addGrid(lr.regParam, [0.10])
             .addGrid(lr.elasticNetParam, [0.10])
             .addGrid(lr.maxIter, [10])
             .build())

evaluator2 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction2", metricName="accuracy")
crossval2 = CrossValidator(estimator=pipeline,
                            estimatorParamMaps=paramGrid2,
                            evaluator=evaluator,
                            numFolds=3)


# COMMAND ----------

cvModel2 = crossval2.fit(training)
# Run cross-validation, and choose the best set of parameters.
# Get best model from cross-validator
bestModel = cvModel2.bestModel
prediction2 = cvModel2.transform(test)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluation and results

# COMMAND ----------

def show_results(model,pred):
    from sklearn.metrics import classification_report
    ########  Make predictions on on the test data
    average_score = cvModel.avgMetrics
    print ('average cross-validation accuracy = {}'.format(average_score[0]))
    ######## Calculate accuracy of the prediction of the test data
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy_score=evaluator.evaluate(prediction)
    # another way to calculate accuracy 
    #correct=prediction.filter(prediction['label']== prediction['prediction2']).select("label","prediction2")
    #accuracy_score = correct.count() / float(test.count())  
    print ('Accuracy in the test data = {}'.format(accuracy_score))
    
    ######## calculate F1 score of the prediction of the test data
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    f1_score=evaluator.evaluate(prediction)
    print ('F1 score in the test data = {}'.format(f1_score))
    # Calculate area under ROC for the prediction of the test data
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    ROC_score=evaluator.evaluate(prediction)
    print ('areaUnderROC in the test data = {}'.format(ROC_score))
    
    ######## Print classification_report
    prediction_and_labels=prediction.select("label","prediction")
    y_true = []
    y_pred = []
    for x in prediction_and_labels.collect():
        xx = list(x)
        try:
            tt = int(xx[1])
            pp = int(xx[0])
            y_true.append(tt)
            y_pred.append(pp)
        except:
            continue

    target_names = ['neg 0', 'pos 1']
    print (classification_report(y_true, y_pred, target_names=target_names))
    return 

# COMMAND ----------

show_results(cvModel2,prediction2)

# COMMAND ----------

# We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
# This will allow us to jointly choose parameters for all Pipeline stages.
# A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
# We use a ParamGridBuilder to construct a grid of parameters to search over.

# Build the parameter grid
paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 50]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

# Create the CrossValidator
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=3)

cvModel = crossval.fit(training)
# Run cross-validation, and choose the best set of parameters.
prediction = cvModel.transform(test)

# COMMAND ----------

# Get best model from cross-validator
bestModel = cvModel.bestModel

# COMMAND ----------

# Compute AUC for test data
evaluator = BinaryClassificationEvaluator()
auc = evaluator.evaluate(bestModel.transform(test))
print("AUC = %g" % auc)

# COMMAND ----------

# Compute accuracy for test data
evaluator = MulticlassClassificationEvaluator(metricName='accuracy')
accuracy = evaluator.evaluate(bestModel.transform(test))
print("Accuracy = %g" % accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving the Models

# COMMAND ----------

import mlflow

with mlflow.start_run():
    mlflow.spark.log_model(cvModel, "model_sentiment_analysis2")

# COMMAND ----------

logged_model = 'runs:/d808cc2da1174a4bbb0642dc2bb7651b/model_sentiment_analysis2'

# Load model
loaded_model = mlflow.spark.load_model(logged_model)


# COMMAND ----------

# Perform inference via model.transform()
prediction_test=loaded_model.transform(test)

# COMMAND ----------

display(prediction_test.select("reviewText","prediction").limit(10))
