import os
import json
import pexpect as pp
import time
import subprocess
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
import subprocess
import subprocess
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


#sign in to azure
def cli_signIn():
    #check if logged in
    os.system("az account list --output tsv | grep True -q || az login")

#create resource group
def createResourceGroup(resourceGroup, location):

    try:
        print("check if resource group exists")
        subprocess.check_output(f"az group list --output tsv | grep {resourceGroup} -q || az group create --name {resourceGroup} --location {location}", stderr=subprocess.STDOUT, shell=True)
        print(f"Resource group {resourceGroup} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating resource group: {e.output.decode()}")
        return None
    return resourceGroup






#create storage account
def createStorageAccount(resourceGroup, storage_account_name, location):
    try:
        print("check if storage account exists")
        subprocess.check_output(f"az storage account list --output tsv \
            | grep {storage_account_name} -q || az storage account create --name {storage_account_name} --resource-group {resourceGroup} --location {location} --sku Standard_LRS"
            , stderr=subprocess.STDOUT, shell=True)
        print(f"Storage account {storage_account_name} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating storage account: {e.output.decode()}")
        return None

    # Define Azure Blob Storage linked service
    storage_account_key = os.popen(f"az storage account keys list --account-name {storage_account_name} --query '[0].value' --output tsv").read().strip()
    return storage_account_name, storage_account_key

#create container
def createContainer(storageAccount, container):
    try:
        print("check if container exists")
        subprocess.check_output(f"az storage container list --account-name {storageAccount} --output tsv \
            | grep {container} -q || az storage container create --name {container} --account-name {storageAccount}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Container {container} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating container: {e.output.decode()}")
        return None
    return container


#create cosmosdb
def createCosmosdb(resourceGroup, cosmosdb, location):

    try:
        print("check if cosmosdb exists")
        subprocess.check_output(f"az cosmosdb list --output tsv | grep {cosmosdb} -q \
            || az cosmosdb create --name {cosmosdb} --resource-group {resourceGroup} \
                                --kind MongoDB --locations regionName={location} failoverPriority=0 isZoneRedundant=False" , stderr=subprocess.STDOUT, shell=True)
        print(f"Cosmosdb {cosmosdb} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating cosmosdb: {e.output.decode()}")
        return None
    return cosmosdb
    


#create cosmosdb database
def createCosmosdbDatabase(resourceGroup, cosmosdb, database):

    try:
        print("check if database exists")
        subprocess.check_output(f"az cosmosdb mongodb database list --account-name {cosmosdb} --resource-group {resourceGroup} --output tsv \
            | grep {database} -q || az cosmosdb mongodb database create --account-name {cosmosdb} --resource-group {resourceGroup} --name {database}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Database {database} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating database: {e.output.decode()}")
        return None
    return database
   

#create cosmosdb collection
def createCosmosdbCollection(resourceGroup, cosmosdb, database, collection):

    try:
        print("check if collection exists")
        subprocess.check_output(f"az cosmosdb mongodb collection list --account-name {cosmosdb} --database-name {database} --resource-group {resourceGroup} --output tsv \
            | grep {collection} -q || az cosmosdb mongodb collection create --account-name {cosmosdb} --database-name {database} --resource-group {resourceGroup} --name {collection}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Collection {collection} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating collection: {e.output.decode()}")
        return None
    return collection






#upload pdf
def uploadPdf(storageAccount, container, pdf, blobPath):
    # Construct the blob name by combining the blob path and the PDF file name
    blobName = f"{blobPath}/{os.path.basename(pdf)}"

    try:
        print("check if blob exists")
        subprocess.check_output(f"az storage blob list --account-name {storageAccount} \
                --container-name {container} --output tsv \
                | grep {blobName} -q \
                || az storage blob upload --account-name {storageAccount} --container-name {container} --name {blobName} --type BlockBlob --file {pdf}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Blob {blobName} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating blob: {e.output.decode()}")
        return None
    return blobName 


    
#create datasets
def createDatasets(resourceGroup, dataFactoryName,datasetName, propertiesJson):

    try:
        print("check if dataset exists")
        subprocess.check_output(f"az datafactory dataset list --factory-name {dataFactoryName} \
                    --resource-group {resourceGroup} --output tsv\
                    | grep {datasetName} -q || az datafactory dataset create --factory-name {dataFactoryName} \
                        --resource-group {resourceGroup} --name {datasetName} --properties @{propertiesJson}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Dataset {datasetName} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating dataset: {e.output.decode()}")
        return None
    return datasetName




#create linked services
def createLinkedServices(resourceGroup, dataFactoryName, linkedServiceName, propertiesJson):

    try:
        print("check if linked service exists")
        subprocess.check_output(f"az datafactory linked-service list --factory-name {dataFactoryName} \
                    --resource-group {resourceGroup} --output tsv\
                    | grep {linkedServiceName} -q || az datafactory linked-service create --factory-name {dataFactoryName} \
                        --resource-group {resourceGroup} --name {linkedServiceName} --properties @{propertiesJson}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Linked service {linkedServiceName} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating linked service: {e.output.decode()}")
        return None
    return linkedServiceName


    



def write_json_file(json_obj, filename):
    if not os.path.exists(f'./{filename}'):
        with open(f'{filename}', 'w') as f:
            json.dump(json_obj, f, indent=2)


#create data factory
def createDataFactory(resourceGroup, dataFactory, location):

    try:
        print("check if data factory exists")
        subprocess.check_output(f"az datafactory list --output tsv | grep {dataFactory} -q \
            || az datafactory create --name {dataFactory} --resource-group {resourceGroup} --location {location}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Data factory {dataFactory} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating data factory: {e.output.decode()}")
        return None
    return dataFactory




#create datasets
def create_datasets(dataset_json_filename, dataset_name, dataFactoryName, resourceGroup):

    try:
        print("check if dataset exists")
        subprocess.check_output(f"az datafactory dataset list --factory-name {dataFactoryName} --output tsv | grep {dataset_name} -q || \
            az datafactory dataset create --factory-name {dataFactoryName} \
                --resource-group {resourceGroup} --name {dataset_name} --properties @{dataset_json_filename}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Dataset {dataset_name} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating dataset: {e.output.decode()}")
        return None
    return dataset_name



def create_linked_services(dataFactoryName, resourceGroup,service_name, sevice_json_filename):

    try:
        print("check if linked service exists")
        subprocess.check_output(f"az datafactory linked-service list --factory-name {dataFactoryName} --resource-group {resourceGroup}  --output tsv | grep {service_name} -q || \
            az datafactory linked-service create --factory-name {dataFactoryName} --resource-group {resourceGroup} --name {service_name} --properties @{sevice_json_filename}" , stderr=subprocess.STDOUT, shell=True)
        print(f"Linked service {service_name} created or already exists")
    except subprocess.CalledProcessError as e:
        print(f"Error creating linked service: {e.output.decode()}")
        return None
    return service_name




def get_blob_linked_service_json(storage_account_name,filename):
    
    # Define Azure Blob Storage linked service
    storage_account_key = os.popen(f"az storage account keys list --account-name {storage_account_name} --query '[0].value' --output tsv").read().strip()

    blob_linked_service_json =  {
                                    "annotations": [],
                                    "type": "AzureBlobFS",
                                    "typeProperties": {
                                        "url": f"https://{storage_account_name}.dfs.core.windows.net/",
                                        "encryptedCredential": storage_account_key
                                    }
                                }

    

    return write_json_file(blob_linked_service_json, filename)

#create_environment_variables
def get_cosmosdb_keys(resourceGroup,cosmosdb_name):
    
    #create environment variables    
    cosmosDbEndpoint_url = os.popen(f"az cosmosdb show --name {cosmosdb_name} --resource-group {resourceGroup} | jq -r '.writeLocations[0].documentEndpoint'").read().strip()
    cosmos_account_key = os.popen(f"az cosmosdb keys list --name {cosmosdb_name} --resource-group {resourceGroup} | jq -r '.primaryMasterKey'").read().strip()
    subscription_id = os.popen(f"az account show | jq -r '.id'").read().strip()
    return {
        
        "cosmosDbEndpoint_url": cosmosDbEndpoint_url,
        "cosmosdb_account_key": cosmos_account_key,
        "subscription_id": subscription_id
    }
    


def get_cosmosDbLinkedService_json(resourceGroup, cosmosdb_name,filename):
    
    cb_dict = get_cosmosdb_keys(resourceGroup,cosmosdb_name)
    cosmosDbEndpoint_url = cb_dict['cosmosDbEndpoint_url']
    cosmos_account_key = cb_dict['cosmosdb_account_key']
    subscription_id = cb_dict['subscription_id']

    # Define Azure Cosmos DB linked service
    cosmosDbLinkedService_json =  {
                                "type": "CosmosDb",
                                "typeProperties": {
                                    "connectionString": f"AccountEndpoint={cosmosDbEndpoint_url};AccountKey={cosmos_account_key};Database=<Database>"
                                }
                            }

    return write_json_file(cosmosDbLinkedService_json, filename)




def create_datasets_jsons(blobStorage_linked_service_name, container_name, cosmosdb_linked_service_name, inputDataset_json_filename, outputDataset_json_filename):
    inputDataset_json = {
                            "type": "Binary",
                            "linkedServiceName": {
                                "referenceName": blobStorage_linked_service_name,
                                "type": "LinkedServiceReference"
                            },
                            "typeProperties": {
                                "location": {
                                    "type": "AzureBlobStorageLocation",
                                    "container": container_name,
                                    "folderPath": "/src",
                                }
                            }
                        }

    

    outputDataset_json = {
                "type": "CosmosDbSqlApiCollection",
                "linkedServiceName":{
                    "referenceName": cosmosdb_linked_service_name,
                    "type": "LinkedServiceReference"
                },
                
                "typeProperties": {
                    "collectionName": "pdf_collection"
                }
            }

    return write_json_file(inputDataset_json, 'inputDataset.json'), write_json_file(outputDataset_json, 'outputDataset.json')



def list_files(startpath):
    """ list all files in a directory"""
    list_files = []
    for root, dirs, files in os.walk(startpath):
        for file in files:
            #print(os.path.join(root, file))
            list_files.append(os.path.join(root, file))
    return [item for item in list_files if '.pdf' in item]

def list_files_container(container_name, storage_account_name, resource_group_name):
    """ list all files in a container"""
    storage_account_key = os.popen(f"az storage account keys list -n {storage_account_name} -g {resource_group_name} --query [0].value -o tsv").read().strip()
    storage_connection_string = os.popen(f"az storage account show-connection-string -g {resource_group_name} -n {storage_account_name} --query connectionString").read().strip()  
    container = ContainerClient.from_connection_string(conn_str=storage_connection_string, container_name=container_name)
    blob_list = container.list_blobs()
    return blob_list


#get a root folder path and a file name and return the full path and uoload the file to the blob storage with destination path prebended
def uploadfilestoBlobStorage(storage_account_name,resource_group_name, container_name, file_path):
    """upload blob storage"""
    storage_account_key = os.popen(f"az storage account keys list -n {storage_account_name} -g {resource_group_name} --query [0].value -o tsv").read().strip()
    storage_connection_string = os.popen(f"az storage account show-connection-string -g {resource_group_name} -n {storage_account_name} --query connectionString").read().strip()  
    container = ContainerClient.from_connection_string(conn_str=storage_connection_string, container_name=container_name)

    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path.replace('..',''))

    #list files in the container already or has been uploaded
    blob_list = container.list_blobs()
    if file_path in blob_list:
        print("file already exist")
        return
    else:
        try:
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
        except Exception as e:
            print(e)
            print("file failed to be uploaded")
            return

def upload_to_blob_storage(pdfpath , storage_account_name, resource_group_name, container_name):
    """upload blob storage"""
    local_files = list_files(pdfpath)
    container_list = [item.name for item in list_files_container(container_name, storage_account_name, resource_group_name)]
    pdf_list = [file for file in local_files if file.replace('..','') not in container_list]
    
    for pdf in pdf_list:
        try:
            uploadfilestoBlobStorage(storage_account_name,resource_group_name, container_name, pdf)
            print(f"file {pdf} uploaded")
        except Exception as e:
            print(e)
            print(f"file {pdf} failed to be uploaded")
            return



def get_resource_subscription_id(resource_group_name):
    return os.popen(f"az group show --name {resource_group_name} | jq -r '.id'").read().strip()



# create service principal 
def create_service_principal(name, role, scope,vault_name, cert_name):
    # check if service principal exists
    try:
        print("check if service principal exists")
        
        cmd = f"az ad sp list --display-name {name} | jq -r '.[0].appId' | grep {name} -q\
            || az ad sp create-for-rbac --name {name} --role {role} --scopes {scope} --keyvault {vault_name} --cert {cert_name} --create-cert"
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
        print(f"Service principal {name} created or already exists")
    except subprocess.CalledProcessError as e:
        print(e)
        return None
    return name

def get_service_principal_appid(service_principal_name):
    try:
        cmd = f" az ad sp list --display-name {service_principal_name} --query '[0].appId'"
        output = os.popen(cmd).read().strip()
    except subprocess.CalledProcessError as e:
        print(e)
        return None
    return output

def get_secret(keyvault_name ="chatkeys", secret_name = "chatgptv2cert"):
    """Get secret from Azure Key Vault"""
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=f"https://{keyvault_name}.vault.azure.net", credential=credential)
    secret = secret_client.get_secret(secret_name)
    return secret.value

def get_tenant_id():
    try:
        cmd = f"az account show --query tenantId -o tsv"
        output = os.popen(cmd).read().strip()
    except subprocess.CalledProcessError as e:
        print(e)
        return None
    return output

def delete_linked_service(datafactoryName, linkedServiceName, resourceGroupName):
    try:
        cmd = f"az datafactory linked-service delete --factory-name {datafactoryName} -n {linkedServiceName} -g {resourceGroupName}"
        print(cmd)
        output = os.popen(cmd).read().strip()
    except subprocess.CalledProcessError as e:
        print(e)
        return None
    return output



def get_hdinsightOnDemandLinkedService_json(hdinsightlinkedService_name, principal_name,
                                            resource_group_name, blobStorage_linked_service_name, filename, clusterType):
    
 
    subscription_id = os.popen("az account show --query id -o tsv").read().strip()
    servicePrincipalID = os.popen(f"az ad sp list --display-name hdinsightPrincipal --query '[].appId'  -o tsv").read().strip()
    servicePrincipalKey = get_secret(keyvault_name="chatkeys", secret_name="chatgptv2cert" )
    tenant_id = get_tenant_id()

    if clusterType == "hadoop":
        clusterType = "hadoop"
        # Define Azure HDInsight OnDemand linked service
        hdinsightOnDemandLinkedService_json =   {
                                                "type": "HDInsightOnDemand",
                                                "typeProperties": {
                                                "clusterType": "hadoop",
                                                "clusterSize": 4,
                                                "timeToLive": "00:15:00",
                                                "hostSubscriptionId": subscription_id,
                                                "servicePrincipalId": servicePrincipalID,
                                                "tenant": tenant_id,
                                                "clusterResourceGroup": resource_group_name,
                                                "version": "3.6",
                                                "osType": "Linux",
                                                "linkedServiceName": {
                                                    "referenceName": blobStorage_linked_service_name,
                                                    "type": "LinkedServiceReference"
                                                }
                                                }
                                            }
    elif clusterType == "spark":
        clusterType = "spark"
        # Define Azure HDInsight OnDemand linked service
        hdinsightOnDemandLinkedService_json =   {
                                                    "type": "HDInsightOnDemand",
                                                    "typeProperties": {
                                                        "clusterSize": 8,
                                                        "clusterType": "spark",
                                                        "timeToLive": "00:00:00",
                                                        "hostSubscriptionId": subscription_id,
                                                        "servicePrincipalId": servicePrincipalID,
                                                        "servicePrincipalKey": {
                                                        "value":servicePrincipalKey,
                                                        "type": "SecureString"
                                                        }
                                                        ,"rootPath": "spark-code"
                                                        ,"entryFilePath": "etl.py",
                                                        "tenant": tenant_id,
                                                        "clusterResourceGroup": resource_group_name,
                                                        "version": "3.6",
                                                        "osType": "Linux",                                                        
                                                        "linkedServiceName": {
                                                        "referenceName": blobStorage_linked_service_name,
                                                        "type": "LinkedServiceReference"
                                                        }
                                                    }
                                                    }

    return write_json_file(hdinsightOnDemandLinkedService_json, filename)

def get_subscription_id(resource_group_name):
    return os.popen(f"az group show --name {resource_group_name} |jq -r .id").read().strip()

def get_user_name():
    return os.popen("az ad signed-in-user show --query userPrincipalName").read().strip()

def assign_role_to_user(user_name, role, scope):
    try:
        cmd = f"az role assignment create --assignee {user_name} --role {role} --scope {scope}"        
        output = os.popen(cmd).read().strip()
    except subprocess.CalledProcessError as e:
        print(e)
        return None
    return output


def create_data_lake_storage (resource_group_name, lake2_storage_account_name, location):
    try:
        cmd = f"az storage account create --name {lake2_storage_account_name} --resource-group {resource_group_name} --location {location}  --kind StorageV2 --hns"
        output = os.popen(cmd).read().strip()
        return lake2_storage_account_name
    except subprocess.CalledProcessError as e:
        print(e)
        return None
    

def create_spark_pipeline_json(dafafactoryName, datafactoryDescription, sparkJoblinkedService_name, hdlinkedService_name, codeContainer_name, codeFile_name):
    json ={
            "name": "Transform data using on-demand HDInsight",
            "properties": {
                "description": datafactoryDescription,
                "activities": [
                    {
                        "name": "ProcessData",
                        "description": datafactoryDescription,
                        "type": "HDInsightSpark",
                        "dependsOn": [],
                        "policy": {
                            "timeout": "7.00:00:00",
                            "retry": 0,
                            "retryIntervalInSeconds": 30,
                            "secureOutput": "false",
                            "secureInput": "false"
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "rootPath": codeContainer_name,
                            "entryFilePath": codeFile_name,
                            "arguments": [
                                "@pipeline().RunId"
                            ],
                            "getDebugInfo": "Always",
                            "sparkConfig": {
                                "runid": {
                                    "value": "@pipeline().RunId",
                                    "type": "Expression"
                                }
                            },
                            "sparkJobLinkedService": {
                                "referenceName": sparkJoblinkedService_name,
                                "type": "LinkedServiceReference"
                            }
                        },
                        "linkedServiceName": {
                            "referenceName": hdlinkedService_name,
                            "type": "LinkedServiceReference"
                        }
                    }
                ],
                "annotations": [],
                "lastPublishTime": "2023-03-23T06:07:45Z"
            },
            "type": "Microsoft.DataFactory/factories/pipelines"
        }
    return json

def main():
    #signin to azure
    cli_signIn()
    


    #create or get resource group
    resource_group_name = createResourceGroup('chatgptGp','eastus')

    #assign role to user
    user_name = get_user_name()
    subscription_id = get_subscription_id(resource_group_name)
    assign_role_to_user(user_name, "Owner", subscription_id)

    #get subscription id
    subscription_id = get_resource_subscription_id(resource_group_name)

    #create lake2 storage account
    storage_account_name = create_data_lake_storage(resource_group_name, 'chatgptv2stn', 'eastus')
    
    # #create storage account
    # storage_account_name,storage_account_key = createStorageAccount('chatgptGp', 'chatgptv2stn', 'eastus')

    # #create a container in the storage account
    container_name = createContainer(storage_account_name, 'chatgpt-ctn')
    container_name = createContainer(storage_account_name, 'etl-code')

    #create a cosmos db account
    cosmosdb_acc_name = createCosmosdb(resourceGroup = resource_group_name, cosmosdb = 'chatgptdb-acn', location='eastus') 
    cosmosdb_databaseName = createCosmosdbDatabase(resourceGroup = resource_group_name, cosmosdb = 'chatgptdb-acn', database = 'chatgptdb-dbn')
    collectionName = createCosmosdbCollection(resourceGroup = resource_group_name, cosmosdb = 'chatgptdb-acn', database = 'chatgptdb-dbn', collection = 'chatgptdb-cln')


    #create a data factory
    datafactoryname = createDataFactory(resourceGroup = resource_group_name, dataFactory = 'chatgpt-df', location = 'eastus')

    #create a linked service
    
    #get cosmosdb values
    cosmos_dict = get_cosmosdb_keys(resourceGroup = resource_group_name ,cosmosdb_name = cosmosdb_acc_name)

    #get resource group id
    resource_group_id = get_resource_subscription_id(resource_group_name)
    #create service principal with role assignment with scope of the resource group and contributor role
    servicePrincipal = create_service_principal(name= 'hdinsightPrincipal', role = 'Contributor', scope = resource_group_id, vault_name= 'chatkeys', cert_name='chatgptv2cert')    


    #create linked services
    get_blob_linked_service_json(storage_account_name = storage_account_name,  filename= 'blob_linked_service.json')
    blobStorage_linked_service_name = create_linked_services(dataFactoryName =datafactoryname  , 
    resourceGroup = resource_group_name,service_name ='blob_service', sevice_json_filename = 'blob_linked_service.json')

    get_cosmosDbLinkedService_json(resourceGroup = resource_group_name, cosmosdb_name = cosmosdb_acc_name,filename = 'cosmosdb_linked_service.json')
    cosmosdb_linked_service_name = create_linked_services(dataFactoryName =datafactoryname  , resourceGroup = resource_group_name,service_name ='cosmosdb_service', sevice_json_filename = 'cosmosdb_linked_service.json')

   

   
    get_hdinsightOnDemandLinkedService_json(hdinsightlinkedService_name ='hdinsight_service_spark', principal_name ='HdinsightOndemand',
                                            resource_group_name = resource_group_name , blobStorage_linked_service_name = 'blob_service' , filename ='hdinsight_service_spark.json', clusterType = 'spark')

    spark_service_name = create_linked_services(dataFactoryName = datafactoryname  , resourceGroup = resource_group_name,service_name ='hdinsight_service_spark', sevice_json_filename = 'hdinsight_service_spark.json')


    #create datasets
    inputDataset_json_filename, outputDataset_json_filename = create_datasets_jsons(blobStorage_linked_service_name = blobStorage_linked_service_name, container_name = container_name, 
    cosmosdb_linked_service_name = cosmosdb_linked_service_name, inputDataset_json_filename = 'inputDataset.json', outputDataset_json_filename = 'outputDataset.json')


    #upload pfs
    upload_to_blob_storage('../src/', storage_account_name = storage_account_name, resource_group_name = resource_group_name, container_name = container_name)    
  
   #upload code
    #upload_to_blob_storage('../src/', storage_account_name = storage_account_name, resource_group_name = resource_group_name, container_name = container_name)    
  







if __name__ == "__main__":
    main()


