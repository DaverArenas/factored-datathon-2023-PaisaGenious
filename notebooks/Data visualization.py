# Databricks notebook source
# Load the Delta table into a DataFrame
df_reviews = spark.read.format("delta").load("s3://1-factored-datathon-2023-lakehouse/Bronze/final_review")


# COMMAND ----------

df = spark.read.format("delta").load("s3://1-factored-datathon-2023-lakehouse/Bronze/final_review")

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
new_dfrev = new_dfrev.withColumn('review_length_chars', F.length('reviewText'))
display(new_dfrev.select('review_length_chars'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distribution of review lenghts in words

# COMMAND ----------

# Calculate the length of each review in words and create a new column 'review_length_words'
new_dfrev = new_dfrev.withColumn('review_length_words', F.size(F.split(new_dfrev['reviewText'], ' ')))
display(new_dfrev.select('review_length_words'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Relationship between length of the review and overall ratings

# COMMAND ----------

# Collect the data to the driver as Pandas DataFrames for plotting
new_dfrev.select('review_length_words', 'overall')
display(new_dfrev.select('review_length_words', 'overall'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Time series analysis

# COMMAND ----------

#average ratings grouped by 'reviewTime')
averageRatingsByDate = new_dfrev \
    .groupBy("unixReviewTime") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("unixReviewTime")

# Display the Results
display(averageRatingsByDate)

# COMMAND ----------

# Extract year and month from the 'unixReviewTime' column
averageRatingsByDate = new_dfrev.withColumn("year", year("unixReviewTime")).withColumn("month", month("unixReviewTime"))

# Group by year and month and calculate the average rating
averageRatingsByMonthYear = averageRatingsByDate \
    .groupBy("year", "month") \
    .agg(F.avg("overall").alias("averageRating")) \
    .orderBy("year", "month")

# Get the last year from the DataFrame
last_year = averageRatingsByMonthYear.select("year").distinct().orderBy("year", ascending=False).limit(1).collect()[0]["year"]

# Filter the data to include only the last five years
averageRatingsLastFiveYears = averageRatingsByMonthYear.filter((averageRatingsByDate["year"] >= last_year-4) & (averageRatingsByMonthYear["year"] <= last_year))

display(averageRatingsLastFiveYears)

# COMMAND ----------


