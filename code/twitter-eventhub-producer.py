# Databricks notebook source
# MAGIC %md # Install EventHub

# COMMAND ----------

# MAGIC %pip install azure-eventhub

# COMMAND ----------

# MAGIC %md # Define Twitter Functions

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
import datetime
import requests
import os
import json

bearer_token = dbutils.secrets.get(scope = "TwitterStreamKV", key = "BearerToken")

def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get("https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth)
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None
    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

def set_rules(delete):
    sample_rules = [
        {"value": "(#UkraineWar OR #WARINUKRAINE) -is:retweet lang:en", "tag": "#UkraineWar tag"}
    ]
    payload = {"add": sample_rules }
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    

# COMMAND ----------

# MAGIC %md # Update Twitter Rules

# COMMAND ----------

rules = get_rules()
delete = delete_all_rules(rules)
set = set_rules(delete)

# COMMAND ----------

from azure.eventhub import EventData, EventHubProducerClient,EventHubSharedKeyCredential
eventhub_policy = dbutils.secrets.get(scope = "TwitterStreamKV", key = "EventHubPolicy")
eventhub_servicebus = dbutils.secrets.get(scope = "TwitterStreamKV", key = "EventHubServicebus")
eventhub_name = dbutils.secrets.get(scope = "TwitterStreamKV", key = "EventHubName")
eventhub_namespace = dbutils.secrets.get(scope = "TwitterStreamKV", key = "EventHubNamespace")
eventhub_sas = dbutils.secrets.get(scope="TwitterStreamKV", key = "EventHubSAS")

sk = EventHubSharedKeyCredential(eventhub_policy,eventhub_sas)
ehpc = EventHubProducerClient(fully_qualified_namespace=eventhub_servicebus, credential=sk,eventhub_name=eventhub_name)

# COMMAND ----------

# MAGIC %md # StreamTweets to EventHub

# COMMAND ----------

response = requests.get(
    "https://api.twitter.com/2/tweets/search/stream?expansions=author_id&tweet.fields=created_at", auth=bearer_oauth, stream=True,
)

if response.status_code != 200:
    raise Exception(
        "Cannot get stream (HTTP {}): {}".format(
            response.status_code, response.text
        )
    )
for response_line in response.iter_lines():
    if response_line:
        event_data_batch = ehpc.create_batch()
        data = EventData(body=response_line)
        event_data_batch.add(data)
        ehpc.send_batch(event_data_batch)
        
ehpc.close()
