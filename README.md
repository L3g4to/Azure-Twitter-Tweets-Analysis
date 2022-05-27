# Azure Twitter Tweets Analysis
The repository contains an example analysis of Twitter tweets captured by an Azure Event Hub using capture (to Azure Blob storage as avro files).

## Prerequisite
To run the analysis you will need the following infrastructure:
- **Azure Blog Storage**
- **Azure EventHub:** You will have to configure your EventHub to capture all events to your Azure Blob Storage
- **Azure Databricks:** To run the notebooks in this repository
- **Azure Key Vault:** For securely storing all secrets

## Overview
All data is saved into Databricks tables to be made available externally for analysis e.g. through Power BI. The following tables are made available:
- All Tweets captured - Tweets captures through the Twitter live API
- Top Tags used and count - analysis of the Tweets captured to the Event Hub
- Top Tweets - most popular Tweets captured through Twitter API on demand and not basis Tweets captured.

## Tutorial
To setup follow instructions below:
* Sign-up for Twitter API https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api
* Find your Bearer Token on your Twitter API
* Setup an Azure Storage Account
* Setup an Azure EventHub (Standard) and configure capturing messages to a Blob Storage location in your Azure Storage Account. The name of your folder should be **twitter**
* Setup a Azure Databricks workspace, create a Cluster (F4 is cheap)
* Setup a Azure Key Vault and configure the following secrets:
  * **BearerToken** - your Twitter Bearer Token
  * **StorageAccountName** - your Azure Storage Account name
  * **BlobStorageSAS** - your Azure Storage Account SAS created key
  * **EventHubNamespace** - the name of your EventHubNamespace
  * **EventHubName** - the name of your EventHub
  * **EventHubPolicy** - the name of the Share Access Policy you create within your EventHub with Manage permissions
  * **EventHubSAS** - the Share Access Policy primary key
  * **EventHubServicebus** - your EventHub Serviuce bus
* Create a SecretScope within your Azure Databricks workspace and name it"TwitterStreamKV". The URL to create a Secret Scope is **https://<databricks-instance>#secrets/createScope**. Use "Creator" as "Manage Principal" and provide your Key Vault DNS name and resource ID.
* Run the **twitter-eventhub-producer** notebook. You should see messages like so:
  ```
  Message 1 received
  Message 2 received
  ```
* In paralell you can now run the **twitter-analysis** notebook. It should successfully create 3 tables:
  * tweets
  * toptags
  * toptweets

  
  
