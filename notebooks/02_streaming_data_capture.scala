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
val maxEventsPerTrigger = 20000

val checkpointPath = "s3://1-factored-datathon-2023-lakehouse/Bronze/eh_streaming/"
val bronze_streaming = "bronze.eh_streaming"

// Set the consumer group and starting position
eventHubsConf.setConsumerGroup(consumerGroup)
eventHubsConf.setStartingPosition(startingPosition)
eventHubsConf.setMaxEventsPerTrigger(maxEventsPerTrigger)

val rowsInEH_Stream = spark.table("bronze.eh_streaming").count()

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
query.awaitTermination(1 * 60 * 1000)

// COMMAND ----------

val newEH_Stream = spark.table("bronze.eh_streaming").count()
if (newEH_Stream >= rowsInEH_Stream + 10000) {
  query.stop()
} 

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*)
// MAGIC from bronze.eh_streaming

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct offset)
// MAGIC from bronze.eh_streaming

// COMMAND ----------

// MAGIC %sql
// MAGIC select max(enqueuedTime)
// MAGIC from bronze.eh_streaming
