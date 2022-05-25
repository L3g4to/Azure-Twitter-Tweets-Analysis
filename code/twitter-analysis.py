# Databricks notebook source
# MAGIC %md # Mount Azure Blob Storage

# COMMAND ----------

try:
  storageAccountName = dbutils.secrets.get(scope = "TwitterStreamKV", key = "StorageAccountName")
  containerName = "twitter"
  dbutils.fs.mount(
    source = f"wasbs://{containerName}@{storageAccountName}.blob.core.windows.net",
    mount_point = "/mnt/twitter",
    extra_configs = {f"fs.azure.account.key.{storageAccountName}.blob.core.windows.net":dbutils.secrets.get(scope = "TwitterStreamKV", key = "BlobStorageSAS")})
  print("Mount installed")
except:
  print("Mount already installed")

# COMMAND ----------

# MAGIC %md # Load data from EventHub Blob storage

# COMMAND ----------

from pyspark.sql.functions import from_json, col, unbase64, explode,regexp_replace,lower,split, trim,length,substring
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DataType, ArrayType,DateType,TimestampType

eventhub_path = "/mnt/twitter/twitterstream/twitterhub/*/*/*/*/*/*/*.avro"

tweetSchema = StructType().add("data", StructType()\
                               .add("author_id", StringType())
                               .add("id",StringType()) \
                               .add("text",StringType()) \
                               .add("public_metrics",StructType().add("retweet_count",IntegerType()).add("like_count",IntegerType())) \
                               .add("created_at",TimestampType())) \
                               .add("includes",StructType() \
                                   .add("users",ArrayType(StructType() \
                                                          .add("username",StringType()) \
                                                          .add("name",StringType()) \
                                                         )) \
                                   )

avroDf = spark.read.format("com.databricks.spark.avro").load(eventhub_path).withColumn('json', from_json(col('Body').cast("string"),tweetSchema)).drop("SequenceNumber", "Offset","EnqueuedTimeUtc","SystemProperties","Properties")
avroDf = avroDf.filter(col("json.data").isNotNull())

# COMMAND ----------

allTweetsDf = avroDf.withColumn("id",col("json.data.id")) \
                  .withColumn("author_id",col("json.data.author_id")) \
                  .withColumn("text",col("json.data.text")) \
                  .withColumn("author_id",col("json.data.text")) \
                  .withColumn("retweet_count",col("json.data.public_metrics.retweet_count")) \
                  .withColumn("like_count",col("json.data.public_metrics.like_count")).withColumn("created_at",col("json.data.created_at"))  \
                  .withColumn("username",col("json.includes.users").getItem(0).getField("username")).drop(col("Body")).drop(col("json"))

# COMMAND ----------

# MAGIC %md # Save All Tweets table

# COMMAND ----------

allTweetsDf.write.saveAsTable("Tweets",mode="overwrite")

# COMMAND ----------

# MAGIC %md # Analysis

# COMMAND ----------

# MAGIC %md ## Find Top Tags

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover 

def cleanUp(str):
  return  trim( \
               regexp_replace(regexp_replace(regexp_replace(str, f"s+",f" "), f"\n",""), "(://([-a-zA-Z0-9()@:%_\+.~#?&//=])+)|(http|https)" , "")
              )
spark.udf.register("cleanUp", cleanUp)
wordDf = avroDf.filter(col("json.data").isNotNull()).select(split(lower(cleanUp(col("json.data.text")))," ").alias("text"))
remover = StopWordsRemover().setInputCol("text").setOutputCol("word")
wordDf = remover.transform(wordDf)
wordDf = wordDf.select(explode(col("text")).alias("word")).filter((col("word") !=  "") & (col("word") != "http") & (substring(col("word"),0,1) =="#" ) ).filter(length(col("word"))>2).groupBy(col("word")).count().sort(col("count").desc())

# COMMAND ----------

# MAGIC %md ## Save Top Tags table

# COMMAND ----------

wordDf.write.saveAsTable("TopTags",mode="overwrite")

# COMMAND ----------

# MAGIC %md ## Get top trending Tweets

# COMMAND ----------

import requests
import json

bearer_token = dbutils.secrets.get(scope = "TwitterStreamKV", key = "BearerToken")

def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r

response = requests.get( "https://api.twitter.com/1.1/search/tweets.json?q=ukraine%20war&result_type=popular", auth=bearer_oauth, stream=True,)

if response.status_code != 200:
    raise Exception(
        "Cannot get stream (HTTP {}): {}".format(
            response.status_code, response.text
        )
    )

# COMMAND ----------

# MAGIC %md # Save Popular Tweets to Table

# COMMAND ----------

tweetSchema = StructType()\
                             .add("id",StringType()) \
                             .add("text",StringType()) \
                             .add("created_at",TimestampType()) \
                             .add("user",StructType() \
                                  .add("name",StringType()) \

                                 ) \
                             .add("retweet_count", IntegerType())                          
topTweetsDf = spark.read.json(sc.parallelize([response.text]))
topTweetsDf = topTweetsDf.withColumn('status', explode(col('statuses'))).drop(col("statuses")).drop(col("search_metadata"))
topTweetsDf = topTweetsDf.withColumn("text",col("status.text"))\
.withColumn("text",col("status.text"))\
.withColumn("created_at",col("status.created_at"))\
.withColumn("username",col("status.user.name"))\
.withColumn("retweet_count",col("status.retweet_count"))\
.drop(col("status"))

# COMMAND ----------

# MAGIC %md # Save Top Tweets Table

# COMMAND ----------

topTweetsDf.write.saveAsTable("TopTweets",mode="overwrite")
