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
import pymongo
from tika import parser
from unidecode import unidecode
from azure.cosmos import exceptions, CosmosClient, PartitionKey
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
    connection_string =    os.popen(f"az cosmosdb keys list --type connection-strings --resource-group {resourceGroup}\
                              --name {cosmosdb_name} | jq '.connectionStrings[0].connectionString' ").read().strip().replace('"','')
    
    return {
        
        "cosmosDbEndpoint_url" : cosmosDbEndpoint_url,
        "masterkey" : masterkey,
        "database_name" : database_name,
        "collection_name" : collection_name,
        "connection_string" : connection_string
    }

def list_filepaths_in_cosmosdb_container():
    client = pymongo.MongoClient(os.environ['connections_string'])
    collection_client = client.get_database(os.environ['database_name']).get_collection(os.environ['collection_name'])
    list = [item['Filepath'] for item in collection_client.find()]
    return list

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





######################    PDF EXTRACTION     ###############################

def extract_title(pdf_path):
    """ extract metatdata from a pdf path"""
    lst = pdf_path.replace('..','').split('/')[1:]
    return lst

# Extract text from a PDF file
def preprocess_text(text):
    # Replace any non-UTF-8 characters with a space
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    return text.strip()

def extract_text_from_container( list_of_pdf_uploaded =list_filepaths_in_cosmosdb_container()):
    print("get environment variables")
    storage_account_name = get_env_vars()['storage_account_name']
    container_name = get_env_vars()['container_name']
    resource_group_name = get_env_vars()['resource_group_name'] 
    storage_connection_string = os.popen(f"az storage account show-connection-string -g {resource_group_name} -n {storage_account_name} --query connectionString").read().strip()
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    return [(item.name,tika_parser(BlobServiceClient.from_connection_string(storage_connection_string).get_blob_client(container=container_name,blob= item.name).download_blob().content_as_bytes()))\
                for item in \
                ContainerClient.from_connection_string(conn_str=storage_connection_string, container_name=container_name).list_blobs()  \
                if item.name not in list_of_pdf_uploaded]
                

# Extract text from a PDF file
def load_blob_into_memory(blob_name):
    """load blob into memory"""
    
    print("get environment variables")
    storage_account_name = get_env_vars()['storage_account_name']
    container_name = get_env_vars()['container_name']
    resource_group_name = get_env_vars()['resource_group_name']    
    storage_connection_string = os.popen(f"az storage account show-connection-string -g {resource_group_name} -n {storage_account_name} --query connectionString").read().strip()

    
    if storage_account_name is None or container_name is None or resource_group_name is None:
        raise Exception("Missing environment variables")
    elif blob_name is None:
        raise Exception("Missing blob name")
        return ''
    else:
        
        try:
            #connection string
            storage_connection_string = os.popen(f"az storage account show-connection-string -g {resource_group_name} -n {storage_account_name} --query connectionString").read().strip()

            # Create blob service client
            blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
            # Get blob client
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)     
            
            # Check if blob exists
            if blob_client.exists():
                # Download blob data
                blob_data = blob_client.download_blob().content_as_bytes()    
                print(f"Successfully downloaded blob: {blob_client.blob_name}")
                return blob_data
            else:
                print(f"Blob {blob_client.blob_name} does not exist")   
               
            
        except Exception as e:
            print(f"Error downloading blob: {e}")

# Extract text from a PDF file
def tika_parser(blob_data):
    try:
        with io.BytesIO(blob_data) as pdf_file:
            # Try to extract text using Tika parser
            try:
                parsed_pdf = parser.from_buffer(pdf_file)
                text = parsed_pdf['content']
                print(f"Successfully extracted {len(text)} characters from PDF using Tika")
                return text
            except:
                pass

            # If Tika fails, try to extract text using pdfplumber
            try:
                with pdfplumber.load(pdf_file) as pdf:
                    pages = pdf.pages
                    text = "\n".join([page.extract_text() for page in pages])
                    print(f"Successfully extracted {len(text)} characters from PDF using pdfplumber")
                    return text
            except:
                
                pass

            # If both Tika and pdfplumber fail, return None
            print("Failed to extract text from PDF using Tika or pdfplumber")
            return None
    except:
        print("Error reading PDF file from memory")
        return ''



def chatgpt3 (userinput, temperature=0.7, frequency_penalty=0, presence_penalty=0):
    """ chat with gpt-3.5-turbo, the much cheaper version of gpt-3"""
    
    suffix = "\n\nTl;dr"
    prompt = userinput+suffix
    assistant_prompt =""
    message = [
        {"role": "user", "content": prompt },        
        {"role": "system", "content": "you are a helpful distinguished scholarly assistant that uses efficient \
         communication to help finish the task of concisely summarizing an article by summarizing the most pertinent essence of the text as part of a paragraph. \
         use the fewest words as possible in english"}
         ]
    openai.api_key = get_env_vars()['OPENAI_API_KEY']
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        temperature=temperature,
        frequency_penalty=frequency_penalty,
        presence_penalty=presence_penalty,
        messages=message
    )
    text = response['choices'][0]['message']['content']
    return text

def cheaper_summarizer(text, title,temperature=0.7, frequency_penalty=0, presence_penalty=0,api_key=None):
    """ chat with gpt-3.5-turbo, the much cheaper version of gpt-3"""        
    
    if text is None:
        print(f"there is no text to summarize - Skipping {title}")
        return ''
    else:
        try:
            print(f"Summarizing {title} for {len(text)} characters")
            #split text into chunks
            chunks = split_text(text)
            max_retry = 3
            retry = 0
            while retry < max_retry:
                try:
                    summaries = ' \n'.join([chatgpt3(chunk, temperature=temperature, frequency_penalty=frequency_penalty, presence_penalty=presence_penalty) for chunk in chunks])
                    break
                except Exception as e:
                    print(f"Exception: {e} - Retrying {title}") 
                    retry += 1
                    sleep(5)
                    continue                
            return summaries
        except Exception as e:
            print(f"Exception: {e} - Skipping {title}")
            return ''
   
                            
    

     
def create_id(folder, typeofDoc, subject, author, title):
    """ create id field for cosmos db """
    # create a string to hash
    my_string = f"{folder}{typeofDoc}{subject}{author}{title}"
    # create a hash object using the SHA-256 algorithm
    hash_object = hashlib.sha256()
    # update the hash object with the string to be hashed
    hash_object.update(my_string.encode())
    # get the hexadecimal representation of the hash
    hex_dig = hash_object.hexdigest()
    return hex_dig

######################    VECTOR DATABASE DATA LOADING     ###############################
def split_text(text: str):
    """ split text into chunks"""
    text_splitter = CharacterTextSplitter()
    chunks = text_splitter.split_text(text)
    return chunks

 
def get_embedding(text, model="text-embedding-ada-002"):
    """ get embedding from openai"""
    openai.api_key = get_env_vars()['OPENAI_API_KEY']
    text = text.replace("\n"," ")
    return openai.Embedding.create(input = [text], model=model)['data'][0]['embedding']

def get_pincone_pdfdata( text,metadata):    
    """ get pinecone pdf data"""
    #create list of vectors
    chunks = split_text(text)   
    
    #create list of pinecone documents
    pinecone_docs = [{"id": hashlib.sha256(item.encode()).hexdigest(),
                        "values": [get_embedding(item )],
                        "metadata": metadata
                         } for item in chunks]  
    return pinecone_docs


######################    UPLOAD DATA     ###############################

def upsert_pinecone_data(vector):
    """ upsert pinecone data"""
    """ upsert pinecone index with pdf data"""
    pinecone.apiKey = get_pinecone_keys()['pinecone.apiKey']
    pinecone.environment = get_pinecone_keys()['pinecone.environment']
    pinecone.indexName = get_pinecone_keys()['pinecone.indexName']
    pinecone.projectName = get_pinecone_keys()['pinecone.projectName']
    
    #initialize pinecone
    pinecone.init(api_key=pinecone.apiKey, env=pinecone.environment)
    
    index = pinecone.Index(pinecone.indexName)
    return index.upsert(vector , namespace=pinecone.projectName)

def write_to_cosmosdb(items):
    
    resource_group_name =  os.environ.get('resource_group_name')
    cosmosdb_acc =         os.environ.get('cosmosdb_acc')
    database_name =        os.environ.get('database_name')
    collection_name =      os.environ.get('collection_name')
    connecting_string = os.popen(f"az cosmosdb keys list --type connection-strings --resource-group {resource_group_name}\
                              --name {cosmosdb_acc} | jq '.connectionStrings[0].connectionString' ").read().strip().replace('"','')
    
    mongo_client = MongoClient(connecting_string)
    collection = mongo_client[database_name][collection_name]

    for item in items:
        print(item['summary'])
        collection.update_one({"id": item["id"]}, {"$set": item}, upsert=True)
