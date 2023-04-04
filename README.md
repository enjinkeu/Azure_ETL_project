# ⚙️ Data Engineering Project ⚙️
## Batch ETL Processing Pipeline with Azure Data Factory and Chatgpt API for Summarizing PDF Files

### Overview

In this project, we will create a batch ETL processing pipeline that uses Azure Data Factory to extract text from PDF files and Chatgpt API to summarize the file. The processed data will then be stored in a Cosmos DB document database.

### Technologies and Skills

- **Azure Data Factory**: Proficient in creating and managing pipelines for ETL processing using Azure Data Factory.
- **Azure Blob Storage**: Expert in managing and configuring Blob Storage for storing large volumes of unstructured data such as PDF files.
- **Chatgpt API**: Skilled in integrating and using Chatgpt API for natural language processing tasks such as summarizing PDF content.
- **Cosmos DB**: Strong knowledge of using Cosmos DB document databases for storing unstructured data in a highly scalable and available manner.
- **Python**: Strong programming skills in Python, with experience in using libraries and tools for data processing and manipulation.
- **API Integration**: Proficient in integrating third-party APIs and services into data processing pipelines.
- **Data Engineering Best Practices**: Knowledge of data engineering patterns, practices, and tools for building robust and maintainable data processing solutions.

### Pipeline Architecture

1. The pipeline starts with a trigger that monitors a specified Blob Storage container for new PDF files.
2. When a new file is detected, Azure Data Factory extracts the text from the PDF file using a Databricks notebook.
3. The extracted text is then passed to Chatgpt API for summarization.
4. The summarized text is stored in a Cosmos DB document database.

### Conclusion

By leveraging Azure Data Factory and Chatgpt API, we have created a scalable batch ETL processing pipeline that extracts text from PDF files, summarizes it using natural language processing, and stores the data in a highly scalable and available Cosmos DB document database. This pipeline can be easily extended to handle additional data sources and destinations, and can be modified to support a wide range of data processing scenarios.
