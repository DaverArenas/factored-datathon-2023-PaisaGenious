-- Databricks notebook source
USE bronze;
CREATE TABLE IF NOT EXISTS reviews_streaming(
    asin                STRING NOT NULL,
    image               STRING,
    overall             STRING,
    reviewText          STRING,
    reviewerID          STRING,
    reviewerName        STRING,
    style               STRING,
    summary             STRING,
    unixReviewTime      STRING,
    verified            STRING,
    vote                STRING,
    offset              STRING,
    sequenceNumber      STRING,
    enqueuedTime        STRING
)
USING DELTA;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMP VIEW events_strings AS 
-- MAGIC SELECT string(body), offset, sequenceNumber, enqueuedTime  
-- MAGIC FROM bronze.eh_streaming;
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMP VIEW parsed_events 
-- MAGIC AS 
-- MAGIC SELECT json.*, offset, sequenceNumber, enqueuedTime
-- MAGIC FROM (
-- MAGIC SELECT from_json(body, schema_of_json('{
-- MAGIC             "asin": "string",
-- MAGIC             "image": "string",
-- MAGIC             "overall": "double",
-- MAGIC             "reviewText": "string",
-- MAGIC             "reviewerID": "string",
-- MAGIC             "reviewerName": "string",
-- MAGIC             "style": "map<string, string>",
-- MAGIC             "summary": "string",
-- MAGIC             "unixReviewTime": "string",
-- MAGIC             "verified": "string",
-- MAGIC             "vote": "string"
-- MAGIC         }')) AS json, offset, sequenceNumber, enqueuedTime
-- MAGIC FROM events_strings);

-- COMMAND ----------

MERGE INTO bronze.reviews_streaming a
USING parsed_events e
ON a.offset = e.offset
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

select count(*)
from bronze.reviews_streaming

-- COMMAND ----------

select count(distinct offset)
from bronze.reviews_streaming
