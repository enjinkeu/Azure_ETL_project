# ⚙️ Data Engineering Project ⚙️
## Batch ETL Processing Pipeline with Azure Data Factory and Chatgpt API for Summarizing PDF Files

### Overview

In this project, we will create a batch ETL processing pipeline that uses Azure Data Factory to extract text from PDF files and Palm API to summarize the file. The processed data will then be stored in a Cosmos DB document database.

### Technologies and Skills

- **Azure Data Factory**: Azure service to create and manage pipelines for ETL processing using Azure Data Factory.
- **Azure Blob Storage**: Azure storage solution for storing large volumes of unstructured data such as PDF files.
- **Chatgpt API**: large language model Chatgpt API for natural language processing tasks such as summarizing PDF content.
- **Cosmos DB**: Azure Cosmos DB solution used as document databases for storing unstructured data in a highly scalable and available manner.
- **Pyspark**: Spark processing to build scalable ETL infrastructure
- **Azure Databricks**: azure streamlined spark ETL workspace to unify ETL for different type of files


### Data Factory Pipeline Architecture

-  **Event-based trigger**: Listens for the BlobCreated event on your Blob Storage container and starts the pipeline when a new file is added to the container.
-  **Copy activity (Data Lake Storage Gen2 to Delta Lake)**: Copies data from your Data Lake Storage Gen2 account to a Delta Lake table on your Databricks cluster.
-  **Databricks Notebook activity**: Runs a transformation on the data in the Delta Lake table using a Databricks notebook.
-  **Copy activity (Delta Lake to Cosmos DB)**: Copies the transformed data from the Delta Lake table to your Cosmos DB account.


### Conclusion

By leveraging Azure Data Factory and Chatgpt API, we have created a scalable batch ETL processing pipeline that extracts text from PDF files, summarizes it using natural language processing, and stores the data in a highly scalable and available Cosmos DB document database. This pipeline can be easily extended to handle additional data sources and destinations, and can be modified to support a wide range of data processing scenarios.
