# ⚙️ Data Engineering Project ⚙️
## Batch ETL Processing Pipeline with Azure Data Factory and Chatgpt API for Summarizing PDF Files

### Overview

In this project, we will create a batch ETL processing pipeline that uses Azure Data Factory to extract text from PDF files and Chatgpt API to summarize the file. The processed data will then be stored in a Cosmos DB document database.

### Technologies and Skills

- **Azure Data Factory**: Azure service to create and manage pipelines for ETL processing using Azure Data Factory.
- **Azure Blob Storage**: Azure storage solution for storing large volumes of unstructured data such as PDF files.
- **Chatgpt API**: large language model Chatgpt API for natural language processing tasks such as summarizing PDF content.
- **Cosmos DB**: Azure Cosmos DB solution used as document databases for storing unstructured data in a highly scalable and available manner.
- **Pyspark**: Spark processing to build scalable ETL infrastructure
- **Azure Databricks**: azure streamlined spark ETL workspace to unify ETL for different type of files


### Pipeline Architecture

1. The pipeline starts with a trigger that monitors a specified Blob Storage container for new PDF files.
2. When a new file is detected, Azure Data Factory extracts the text from the PDF file using a Databricks notebook.
3. The extracted text is then passed to Chatgpt API for summarization.
4. The summarized text is stored in a Cosmos DB document database.

### Conclusion

By leveraging Azure Data Factory and Chatgpt API, we have created a scalable batch ETL processing pipeline that extracts text from PDF files, summarizes it using natural language processing, and stores the data in a highly scalable and available Cosmos DB document database. This pipeline can be easily extended to handle additional data sources and destinations, and can be modified to support a wide range of data processing scenarios.
