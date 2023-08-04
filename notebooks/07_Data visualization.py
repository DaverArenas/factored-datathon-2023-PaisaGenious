# Databricks notebook source
# Load the Delta table into a DataFrame
df_reviews = spark.read.format("delta").load("s3://1-factored-datathon-2023-lakehouse/gold/review_cleaned2")


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import lower,col,regexp_replace,when,rand,regexp_extract
from pyspark.sql.types import IntegerType, StringType,FloatType
import re
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
import html


# COMMAND ----------

union_df = spark.table("gold.reviews_details")

# COMMAND ----------

display(union_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of overall rating

# COMMAND ----------

# Count the occurrences of each rating value
rating_counts = df_reviews.groupBy('overall').count().orderBy('overall')
display(rating_counts)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Relationship between verified purchases and overall ratings

# COMMAND ----------

# Filter data for verified purchases with 'overall' rating
verified_purchases = df_reviews.select('verified','overall')

verified_purchases =verified_purchases.groupBy("overall", "verified").count().orderBy("overall")
# Show the DataFrame with both verified and not verified purchases and their 'overall' ratings
display(verified_purchases)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of review lenghts in characters

# COMMAND ----------

# Calculate the length of each review in characters
display(df_reviews.select('review_length_chars'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of review lenghts in words

# COMMAND ----------

# Calculate the length of each review in words and create a new column 'review_length_words'
display(df_reviews.select('review_length_words'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Relationship between length of the review and overall ratings

# COMMAND ----------

display(df_reviews.select('review_length_words', 'overall'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Average of ratings per year

# COMMAND ----------

#average ratings grouped by 'reviewTime')
averageRatingsByDate = df_reviews\
    .groupBy("year") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("year")

# Display the Results
display(averageRatingsByDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Average of ratings per month

# COMMAND ----------

averageRatingsByMonth = df_reviews\
    .groupBy("month") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("month")

# Display the Results
display(averageRatingsByMonth)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reviews per year

# COMMAND ----------

ReviewsYear = df_reviews\
    .groupBy("year").count().orderBy("year")

# Display the Results
display(ReviewsYear)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reviews per month

# COMMAND ----------

ReviewsMonth = df_reviews\
    .groupBy("month").count()

# Display the Results
display(ReviewsMonth)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sentiment analysis

# COMMAND ----------

# Add a new column "sentiment_label" based on sentiment polarity
union_df = df_reviews.withColumn("sentiment", 
                                   when(col("overall") < 3.0, "Negative")
                                   .when(col("overall") == 3.0, "Neutral")
                                   .when(col("overall") > 3.0, "Positive"))
display(union_df.groupBy('sentiment').count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sentiment per year

# COMMAND ----------

display(union_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Data imputation - main_cat

# COMMAND ----------

def unescape_html(text):
    if text is not None:
        return html.unescape(text)

unescape_udf = udf(unescape_html, StringType())

new_df = union_df.withColumn("main_cat", when(col("main_cat").isNull(), "undefined").otherwise(unescape_udf(col("main_cat"))))
#new_df= union_df.withColumn("main_cat", unescape_udf(col("main_cat")))

new_df = new_df.withColumn("extracted_cat", when(col("main_cat")\
    .like('<img src=%'), regexp_extract(col("main_cat"), 'alt="([^"]+)"', 1)).otherwise(col("main_cat")))

new_df = new_df.withColumn("extracted_cat",F.when(F.col("extracted_cat")=="undefined", F.col('category').getItem(0))\
    .otherwise(F.col("extracted_cat")))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Data imputation - price

# COMMAND ----------

# Define a function to calculate the average price
def calculate_average_price(price):

    if price is not None:
        # Regular expression to match price ranges
        range_pattern = r"\$(\d+(?:\.\d+)?)\s*-\s*\$(\d+(?:\.\d+)?)"
        if "-" in price:
            range_match = re.match(range_pattern, price)
            if range_match:
                try:
                    min_price = float(range_match.group(1))
                    max_price = float(range_match.group(2))
                    avg_price = (min_price + max_price) / 2
                    return avg_price
                except (ValueError, IndexError):
                    return None
        elif "$" in price:
            try:
                return float(price.strip('$'))
            except ValueError:
                return None
        
        elif price=="":
            return None


        
# Register the function as a UDF (User-Defined Function)
calculate_avg_price_udf = F.udf(calculate_average_price, FloatType())

# Create a new column "average_price" using the UDF to calculate the average price
#df =new_df.withColumn("average_price", when(col("price").isNull(), 0).otherwise(calculate_avg_price_udf(F.col("price"))))
df = new_df.withColumn("average_price", calculate_avg_price_udf(F.col("price")))

# COMMAND ----------

# Step 1: Calculate the mean for each "main_cat" category
mean_df = df.groupBy("main_cat").agg(F.round(F.avg("average_price"),2).alias("mean_price"))

# Step 2: Join the original DataFrame with the mean DataFrame to get the mean_price for each main_cat
df = df.join(mean_df, on="main_cat", how="left")

# Step 3: Replace null values in "average_price" column with the corresponding mean_price
df = df.withColumn("average_price", F.when(F.col("average_price").isNull(), F.col("mean_price"))
    .otherwise((F.col("average_price"))))

#mean_value_total = df.selectExpr("mean(average_price)").collect()[0][0]

# Paso 4: impute the rest of null values with the total average 
#df = df.withColumn("average_price", col("average_price").fillna(mean_value_total))

# Step 5: Drop the "mean_price" column, as it's no longer needed
df = df.drop("mean_price")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Number of reviews per category

# COMMAND ----------

df_grouped = df.groupBy("extracted_cat").agg(F.count("reviewText").alias("review_count"))

# Order the DataFrame by the count in descending order
df_sorted = df_grouped.orderBy(F.desc("review_count"))
display(df_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Overall average per category

# COMMAND ----------

averageRatingsByMainCat = df\
    .groupBy("extracted_cat") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy(F.desc("averageRating"))

# Display the Results
display(averageRatingsByMainCat)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Distribution of price

# COMMAND ----------

display(df.select("average_price"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Negative reviews analysis

# COMMAND ----------

negative_reviews_df = df.filter((F.col("overall") < 3))
display(negative_reviews_df.select("average_price","brand","extracted_cat","asin","title"))

# COMMAND ----------

count_brand=negative_reviews_df.groupBy("brand").count().orderBy(F.desc("count"))
top_10_products_n = count_brand.limit(10)
display(top_10_products_n)

# COMMAND ----------

count_title=negative_reviews_df.groupBy("title").count().orderBy(F.desc("count"))
top_10_products_n = count_title.limit(10)
display(top_10_products_n)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Positive reviews analysis

# COMMAND ----------

positive_reviews_df = df.filter((F.col("overall") > 3))
display(positive_reviews_df.select("average_price","brand","extrated_cat","asin","title"))

# COMMAND ----------

count_brand_p=positive_reviews_df.groupBy("brand").count().orderBy(F.desc("count"))
top_10_products_p = count_brand_p.limit(10)
display(top_10_products_p)

# COMMAND ----------

count_title_p=positive_reviews_df.groupBy("title").count().orderBy(F.desc("count"))
top_10_products_p= count_title_p.limit(10)
display(top_10_products_p)

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("append") \
    .option("overwriteSchema", "true") \
    .save("s3://1-factored-datathon-2023-lakehouse/Bronze/review_metadata")

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("gold.amazon_reviews_")
