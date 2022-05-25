# Azure Twitter Tweets Analysis
The repository contains an example analysis of Twitter tweets captured by an Azure Event Hub using capture (to Azure Blob storage as avro files).

## Prerequisite
To run the analysis you will need the following infrastructure:
- **Azure Blog Storage**
- **Azure EventHub:** You will have to configure your EventHub to capture all events to your Azure Blob Storage
- **Azure Databricks:** To run the notebooks in this repository
- **Azure KeyValut:** For securely storing all secrets

## Overview
All data is saved into Databricks tables to be made available externally for analysis e.g. through Power BI. The following tables are made available:
- All Tweets captured - Tweets captures through the Twitter live API
- Top Tags used and count - analysis of the Tweets captured to the Event Hub
- Top Tweets - most popular Tweets captured through Twitter API on demand and not basis Tweets captured.
