import sys
from utils import *
sys.modules['pkg_resources'] = None



def main():

    #set variables to be used in this ETL process

    print("set environment variables")
    os.environ['OPENAI_API_KEY'] = get_secret("chatKeys", "openaiKey")     
    pinecone_dict = get_pinecone_keys()
    pinecone_jar, cosmos_jar = set_spark_liraries()
    cosmos_dict = get_cosmosdb_keys(resourceGroup = "chatgptGp",cosmosdb_name="chatgptdb-acn")
    os.environ['OPENAI_API_KEY'] = get_secret(keyvault_name='chatkeys', secret_name='openaiKey')
    
    os.environ['storage_account_name'] = 'chatgptv2stn'
    os.environ['container_name'] = 'chatgpt-ctn'
    os.environ['resource_group_name'] ='chatgptGp'
    os.environ['cosmosdb_acc'] ='chatgptdb-acn'
    os.environ['database_name']='chatgptdb-dbn'
    os.environ['collection_name']='chatgptdb-cln'    
    
    #blob storage loading

    print("load blob storage")
    pdf_paths = [item for item in list_pdfblobs()][:1]
    print(f"number of pdf files: {len(pdf_paths)}")

    if len(pdf_paths) == 0:
        print("no pdfs to process")
        return
    else:

        print("starting spark session")    
        spark = SparkSession.builder\
            .appName("chatgpt")\
            .config("spark.jars.packages", f"{pinecone_jar}")\
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        print("create the cosmosdb rdd")
        preprocess_text_rdd = spark.sparkContext.parallelize(pdf_paths)\
                                            .map(lambda x: (x, tika_parser(load_blob_into_memory(x))))
                                                    
    print(preprocess_text_rdd.take(1))



    




if __name__ == "__main__":
    main()