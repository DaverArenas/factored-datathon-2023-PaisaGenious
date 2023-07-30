// Databricks notebook source
import org.apache.spark.eventhubs.ConnectionStringBuilder
import org.apache.spark.eventhubs.EventHubsConf
import com.microsoft.azure.eventhubs.EventPosition

// COMMAND ----------

// DBTITLE 1,Create a connection string and an EventHubsConf object to connect to Azure Event Hub. Connection String with EntityPath
val connectionString = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_group_3;SharedAccessKey=JLEggz9GNlDdLvbypDAudzTABp+WnVeIY+AEhBAupi4=;EntityPath=factored_datathon_amazon_reviews_3"

val eventHubsConf = EventHubsConf(connectionString)

// COMMAND ----------

// Additional Configurations:
val consumerGroup = "paisa_genious" // Consumer group assigned to the team ("Paisa Genious")
val startingPosition = org.apache.spark.eventhubs.EventPosition.fromStartOfStream // Starting position for your streaming job

// Set the consumer group and starting position
eventHubsConf.setConsumerGroup(consumerGroup)
eventHubsConf.setStartingPosition(startingPosition)

// COMMAND ----------

// Optional: Set the maxEventsPerTrigger based on the desired number of records to process per trigger interval.
val maxEventsPerTrigger = 50000 // Set to the number of records sent per day (10,000) for your team
eventHubsConf.setMaxEventsPerTrigger(maxEventsPerTrigger)

// COMMAND ----------

val df = spark
  .readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

val s3_base_path = "s3://1-factored-datathon-2023-lakehouse"
val checkpointPath = "s3://1-factored-datathon-2023-lakehouse/Bronze/amazon_streaming/"
val bronze_streaming = "bronze.amazon_streaming"

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val query = df
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .option("mergeSchema", "true")
  .trigger(Trigger.AvailableNow()) // Use Trigger.Once() for availableNow=True
  .toTable(bronze_streaming)

// Start the structured streaming job.
query.awaitTermination()

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*)
// MAGIC from bronze.amazon_streaming
// MAGIC --limit 1000

// COMMAND ----------

// MAGIC %sql
// MAGIC select *
// MAGIC from bronze.amazon_streaming
// MAGIC limit 1000
