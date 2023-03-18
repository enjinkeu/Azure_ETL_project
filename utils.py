##################################################
#                                                #   
# this file will store all the utility functions #
#                                                #
#################################################
import sys
import io
import os
import re
import json
import string
import hashlib
import datetime
from time import time, sleep
from typing import List

import openai
import pinecone
import pdfplumber
import tiktoken
from tika import parser
from unidecode import unidecode
from pymongo import MongoClient
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.cosmos import CosmosClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
from langchain.text_splitter import CharacterTextSplitter

def cli_signIn():
    """function to login to azure cli if not already logged in"""
    os.system("az account list --output tsv | grep True -q || az login")

######################    SECRETS       ###############################

def get_secret(keyvault_name ="chatkeys", secret_name = "openaiKey"):
    """Get secret from Azure Key Vault"""
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=f"https://{keyvault_name}.vault.azure.net", credential=credential)
    secret = secret_client.get_secret(secret_name)
    return secret.value

def get_pinecone_keys():
    """ get pinecone keys from azure"""
    pinecone_api_key = get_secret(keyvault_name ="chatkeys", secret_name = "pinecone")
    pinecone_env = get_secret(keyvault_name ="chatkeys", secret_name = "pineconeEnv")
    pinecone_index = get_secret(keyvault_name="chatKeys", secret_name="pineconeIdx")

    return {
        "pinecone.apiKey": pinecone_api_key,
        "pinecone.environment": pinecone_env,
        "pinecone.indexName": pinecone_index,
        "pinecone.projectName": "chatgpt3"
        
    }
        
def get_cosmosdb_keys(resourceGroup,cosmosdb_name):
    """ get cosmos db keys from azure"""
    #create environment variables  
    cosmosDbEndpoint_url = os.popen(f"az cosmosdb show --resource-group {resourceGroup}  --name {cosmosdb_name} --query 'writeLocations[].documentEndpoint' -o tsv").read().strip()
    cosmos_account_key =   os.popen(f"az cosmosdb keys  list --name {cosmosdb_name} --resource-group {resourceGroup} | jq -r '.primaryMasterKey'").read().strip()    
    database_name =        os.popen(f"az cosmosdb database list --name {cosmosdb_name} --resource-group {resourceGroup} | jq -r '.[0].id'").read().strip()
    collection_name =       os.popen(f"az cosmosdb collection list --name {cosmosdb_name} --db-name {database_name} --resource-group {resourceGroup} | jq -r '.[0].id'").read().strip()
    masterkey =            os.popen(f" az cosmosdb list-keys --name {cosmosdb_name} --resource-group {resourceGroup} --query primaryMasterKey").read().strip()
    
    return {
        
        "cosmosDbEndpoint_url" : cosmosDbEndpoint_url,
        "masterkey" : masterkey,
        "database_name" : database_name,
        "collection_name" : collection_name,
        "writingBatchSize":"1000",
        "Upsert": "true"
    }

def set_spark_liraries():
        #packages to load in spark session
        group_id = "io.pinecone"
        artifact_id = "spark-pinecone_2.13"
        version = "0.1.1"

        pkg1 = f"{group_id}:{artifact_id}:{version}"

        group_id = "com.azure.cosmos.spark"
        artifact_id = "azure-cosmos-spark_3-3_2-12"
        version = "4.17.2"

        pkg2 = f"{group_id}:{artifact_id}:{version}"

        return pkg1, pkg2

def get_env_vars():
    env_dict = {
        "resource_group_name": os.environ['resource_group_name'],
        "storage_account_name": os.environ['storage_account_name'],
        "container_name": os.environ['container_name'],
        "cosmosdb_acc": os.environ['cosmosdb_acc'],
        "database_name": os.environ['database_name'],
        "collection_name": os.environ['collection_name'],
        "OPENAI_API_KEY": os.environ['OPENAI_API_KEY'],
    }
    for k, v in env_dict.items():
        if v is None:
            raise Exception(f"{k} environment variable is not set")
    return env_dict


######################    FILE MANAGEMENT       ###############################

def list_files(startpath):
    """ list all files in a directory"""
    list_files = []
    for root, dirs, files in os.walk(startpath):
        for file in files:
            #print(os.path.join(root, file))
            list_files.append(os.path.join(root, file))
    return [item for item in list_files if '.pdf' in item]

def list_pdfblobs():

    storage_account_name = get_env_vars()['storage_account_name']
    resource_group_name = get_env_vars()['resource_group_name']
    container_name = get_env_vars()['container_name']
    """list pdf blobs in blob storage"""
    list_of_uploaded_files = [item['Filepath'] for item in get_cosmosdb_uploaded_files().find({})]
    storage_account_key = os.popen(f"az storage account keys list -n {storage_account_name} -g {resource_group_name} --query [0].value -o tsv").read().strip()
    storage_connection_string = os.popen(f"az storage account show-connection-string -g {resource_group_name} -n {storage_account_name} --query connectionString").read().strip()  
    container = ContainerClient.from_connection_string(conn_str=storage_connection_string, container_name=container_name)
    blob_list = container.list_blobs()
    #https://<your-storage-account-name>.blob.core.windows.net/<your-container-name>/<your-blob-name>

    blob_list =  [item['name'] for item in blob_list if item['name'].endswith('.pdf')  if item['name'] not in list_of_uploaded_files]
    print(blob_list)
    
    return blob_list

def delete_cosmosdb_uploaded_files():
    """ get cosmos db keys from azure"""
    resource_group_name = get_env_vars()['resource_group_name']
    cosmosdb_acc = get_env_vars()['cosmosdb_acc']
    database_name = get_env_vars()['database_name']
    collection_name = get_env_vars()['collection_name']

    connecting_string = os.popen(f"az cosmosdb keys list --type connection-strings --resource-group {resource_group_name}\
                              --name {cosmosdb_acc} | jq .connectionStrings[0].connectionString ").read().strip().replace('"','')
    collection_name = MongoClient(connecting_string)[database_name][collection_name]
    
    return collection_name.delete_many({})


def get_cosmosdb_uploaded_files():
    """ get cosmos db keys from azure""" 
    resource_group_name = get_env_vars()['resource_group_name']
    cosmosdb_acc = get_env_vars()['cosmosdb_acc']  
    database_name=get_env_vars()['database_name'] 
    collection_name=get_env_vars()['collection_name']   
    
    connecting_string = os.popen(f"az cosmosdb keys list --type connection-strings --resource-group {resource_group_name}\
                              --name {cosmosdb_acc} | jq '.connectionStrings[0].connectionString' ").read().strip().replace('"','')
    collection_name = MongoClient(connecting_string)[database_name][collection_name]
    
    return collection_name


######################    PDF EXTRACTION     ###############################

def extract_title(pdf_path):
    """ extract metatdata from a pdf path"""
    lst = pdf_path.replace('..','').split('/')[1:]
    return lst


# Extract text from a PDF file
def load_blob_into_memory(blob_name):
    """load blob into memory"""
    
    print("get environment variables")
    storage_account_name = get_env_vars()['storage_account_name']
    container_name = get_env_vars()['container_name']
    resource_group_name = get_env_vars()['resource_group_name']
    

    if storage_account_name is None or container_name is None or resource_group_name is None:
        raise Exception("Missing environment variables")
    else:
        
        try:
            #connection string
            storage_connection_string = os.popen(f"az storage account show-connection-string -g {resource_group_name} -n {storage_account_name} --query connectionString").read().strip()

            # Create blob service client
            blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
            # Get blob client
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)         
            # Download blob data
            blob_data = blob_client.download_blob().content_as_bytes()    
            print(f"Successfully downloaded blob: {blob_name}")
            return blob_data
        except Exception as e:
            print(f"Error downloading blob: {e}")

# Extract text from a PDF file
def tika_parser(blob_data):
    """ extract text from a pdf blob with tika parser
        use pdfplumber as a fallback if tika fails
    """
    
    try:
        with io.BytesIO(blob_data) as pdf_file:
            # Try to extract text using Tika parser
            print("first try tika parser")
            try:
                print(pdf_file)
                parsed_pdf = parser.from_buffer(pdf_file)
                text = parsed_pdf['content']
                print(text)
                print(f"Successfully extracted {len(text)} characters from PDF using Tika")
                return text
            except Exception as e:
                print(f"Error extracting text from PDF using Tika: {e}")
                pass

            # If Tika fails, try to extract text using pdfplumber
            try:
                print("second try pdfplumber")
                with pdfplumber.load(pdf_file) as pdf:
                    pages = pdf.pages
                    text = "\n".join([page.extract_text() for page in pages])
                    print(f"Successfully extracted {len(text)} characters from PDF using pdfplumber")
                    return text
            except Exception as e:
                print(f"Error extracting text from PDF using pdfplumber: {e}")                
                pass

            # If both Tika and pdfplumber fail, return None
            print("Failed to extract text from PDF using Tika or pdfplumber")
            return None
    except:
        print("Error reading PDF file from memory")
        return None



# Extract text from a PDF file
def preprocess_text(text):
    # Replace any non-UTF-8 characters with a space
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    return text.strip()
