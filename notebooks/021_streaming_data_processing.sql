-- Databricks notebook source
USE bronze;
CREATE TABLE IF NOT EXISTS amazon_stream(
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
-- MAGIC FROM bronze.EH_Stream;
-- MAGIC
-- MAGIC CREATE OR REPLACE TEMP VIEW parsed_events 
-- MAGIC AS 
-- MAGIC SELECT json.*, offset, sequenceNumber, enqueuedTime
-- MAGIC FROM (
-- MAGIC SELECT from_json(body, schema_of_json(replace('{"asin":"B00005QDPX","image":null,"overall":5.0,"reviewText":"Crazy Taxi is by far one of the best video games I\'ve ever played. The soundtrack, gameplay, and replay value are insane! There are only 2 levels, but it is still fun to play over and over and over again! Basically, the objective is to drive around and take customers to the place they want to go. Boring, right? Wrong! Drive as crazy as you like, from crashing into cars to jumping over stuff, or just driving all around crazy! If you don\'t make it to the customer\'s destination in time, they\'ll jump out of the car, which is pretty funny. There are four drivers, and they all have one heck of an attitude. My personal fav is Gus. He\'s old, but he always plays it cool. Many people think S is the best license you can get, but after that, it\'s awesome and after that, it\'s crazy. There are three modes, arcade, original, and crazy box. Arcade and original are basically the same, except the course on which you play. Crazy Box is a series of minigames. Overall, This Game rules! Unless you\'ve already bought it, buy it now while you can. Despite all the bad reviews for this game, it rocks, and you should buy it. Nintendo rules!","reviewerID":"A36TDX8DY2XK5Q","reviewerName":"Some Kid","style":null,"summary":"It\'s Party Time! Let\'s Have Some Fun!","unixReviewTime":1054252800,"verified":false,"vote":null}', "'", "\\'"))) AS json, offset, sequenceNumber, enqueuedTime
-- MAGIC FROM events_strings);

-- COMMAND ----------

MERGE INTO bronze.amazon_stream a
USING parsed_events e
ON a.offset = e.offset
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

SELECT*
FROM bronze.amazon_reviews
LIMIT 10

-- COMMAND ----------

select count(*)
from bronze.amazon_stream
