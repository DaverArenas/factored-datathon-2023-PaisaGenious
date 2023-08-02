// Databricks notebook source
import org.apache.spark.eventhubs.ConnectionStringBuilder
import org.apache.spark.eventhubs.EventHubsConf
import com.microsoft.azure.eventhubs.EventPosition
import org.apache.spark.sql.streaming.Trigger

// COMMAND ----------

// DBTITLE 1,Create a connection string and an EventHubsConf object to connect to Azure Event Hub. Connection String with EntityPath
val connectionString = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_group_3;SharedAccessKey=JLEggz9GNlDdLvbypDAudzTABp+WnVeIY+AEhBAupi4=;EntityPath=factored_datathon_amazon_reviews_3"

val eventHubsConf = EventHubsConf(connectionString)

// Additional Configurations:
val consumerGroup = "paisa_genious" // Consumer group assigned to the team ("Paisa Genious")
val startingPosition = org.apache.spark.eventhubs.EventPosition.fromStartOfStream // Starting position for your streaming job
val maxEventsPerTrigger = 50000

val checkpointPath = "s3://1-factored-datathon-2023-lakehouse/Bronze/EH_Stream/"
val bronze_streaming = "bronze.EH_Stream"

// Set the consumer group and starting position
eventHubsConf.setConsumerGroup(consumerGroup)
eventHubsConf.setStartingPosition(startingPosition)
eventHubsConf.setMaxEventsPerTrigger(maxEventsPerTrigger)

val df = spark
  .readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

val query = df
.writeStream
.option("checkpointLocation", checkpointPath)
.option("mergeSchema", "true")
.trigger(Trigger.ProcessingTime("1 minute"))
.toTable(bronze_streaming)

// Start the structured streaming job.
query.awaitTermination()

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*)
// MAGIC from bronze.EH_Stream

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct offset)
// MAGIC from bronze.amazon_streaming

// COMMAND ----------

// MAGIC %sql
// MAGIC select *
// MAGIC from bronze.amazon_streaming
// MAGIC limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select max(enqueuedTime)
// MAGIC from bronze.amazon_streaming

// COMMAND ----------

// MAGIC %python 
// MAGIC from pyspark.sql.functions import col
// MAGIC df = spark.read.table("bronze.amazon_streaming")

// COMMAND ----------

selected_df = df.select(col("asin"), col("overall"), col("reviewText"))


// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW events_strings AS 
// MAGIC SELECT string(body), offset, sequenceNumber, enqueuedTime  
// MAGIC FROM bronze.amazon_streaming;
// MAGIC
// MAGIC SELECT * 
// MAGIC FROM events_strings
// MAGIC LIMIT 100

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW parsed_events 
// MAGIC AS 
// MAGIC SELECT json.*, offset, sequenceNumber, enqueuedTime
// MAGIC FROM (
// MAGIC SELECT from_json(body, schema_of_json(replace('{"asin":"B00005QDPX","image":null,"overall":5.0,"reviewText":"Crazy Taxi is by far one of the best video games I\'ve ever played. The soundtrack, gameplay, and replay value are insane! There are only 2 levels, but it is still fun to play over and over and over again! Basically, the objective is to drive around and take customers to the place they want to go. Boring, right? Wrong! Drive as crazy as you like, from crashing into cars to jumping over stuff, or just driving all around crazy! If you don\'t make it to the customer\'s destination in time, they\'ll jump out of the car, which is pretty funny. There are four drivers, and they all have one heck of an attitude. My personal fav is Gus. He\'s old, but he always plays it cool. Many people think S is the best license you can get, but after that, it\'s awesome and after that, it\'s crazy. There are three modes, arcade, original, and crazy box. Arcade and original are basically the same, except the course on which you play. Crazy Box is a series of minigames. Overall, This Game rules! Unless you\'ve already bought it, buy it now while you can. Despite all the bad reviews for this game, it rocks, and you should buy it. Nintendo rules!","reviewerID":"A36TDX8DY2XK5Q","reviewerName":"Some Kid","style":null,"summary":"It\'s Party Time! Let\'s Have Some Fun!","unixReviewTime":1054252800,"verified":false,"vote":null}', "'", "\\'"))) AS json, offset, sequenceNumber, enqueuedTime
// MAGIC FROM events_strings);
// MAGIC
// MAGIC SELECT * 
// MAGIC FROM parsed_events
// MAGIC LIMIT 100;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*)
// MAGIC FROM parsed_events

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(DISTINCT offset)
// MAGIC FROM bronze.EH_Stream

// COMMAND ----------


