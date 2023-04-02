import sys
from utils import *
sys.modules['pkg_resources'] = None



def main():

    #set variables to be used in this ETL process

    print("set environment variables")
    os.environ['OPENAI_API_KEY'] = get_secret("chatKeys", "openaiKey")   
    os.environ['storage_account_name'] = 'chatgptv2stn'
    os.environ['container_name'] = 'chatgpt-ctn'
    os.environ['resource_group_name'] ='chatgptGp'
    os.environ['cosmosdb_acc'] ='chatgptdb-acn'
    os.environ['database_name']='chatgptdb-dbn'
    os.environ['collection_name']='chatgptdb-cln'        
    pinecone_dict = get_pinecone_keys()
    pinecone_jar, cosmos_jar = set_spark_liraries()    
    
    
    

        
    #blob storage loading

    print("load blob storage")
    pdf_paths = [item for item in list_pdfblobs()][:1]
    print(f"number of pdf files: {len(pdf_paths)}")

    
    print("starting spark session")    
    spark = SparkSession.builder\
        .appName("chatgpt")\
        .config("spark.jars.packages", f"{pinecone_jar}")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print(pdf_paths)        
    
    print("create the cosmosdb rdd")

    preprocess_text_list =  spark.sparkContext.parallelize(extract_text_from_container()).collect()



    print(f"number of pdf files: {len(preprocess_text_list)}")
    
    load_to_cosmosdb_rdd = spark.sparkContext.parallelize(preprocess_text_list[:5]).map(lambda x: ('/'+x[0], preprocess_text(x[1]))).\
                                    filter(lambda x: len(x[1]) >100 ).\
                                    map(lambda x: {
                                            "Filepath": x[0],
                                            "Metadata": {
                                                "folder": extract_title(x[0])[0],
                                                "typeofDoc": extract_title(x[0])[1],
                                                "subject": extract_title(x[0])[2],
                                                "author": extract_title(x[0])[3],
                                                "title": extract_title(x[0])[4].split('.')[0]
                                            },
                                            "text": x[1],
                                            "summary": cheaper_summarizer(x[1], extract_title(x[0])),
                                            "id": create_id(
                                                extract_title(x[0])[0],  # folder
                                                extract_title(x[0])[1],  # typeofDoc
                                                extract_title(x[0])[2],  # subject
                                                extract_title(x[0])[3],  # author
                                                extract_title(x[0])[4].split('.')[0],  # title
                                            ),
                                            "uploadDate": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        }).foreachPartition(lambda x: write_to_cosmosdb(x))
    

    # print("create the pinecone rdd and write to pinecone")
    # load_data_to_pinecone_rdff = spark.sparkContext.parallelize(get_data_from_cosmosdb()).map(lambda x : ([x['text'],x['Metadata']])).\
    #                                             map(lambda x : get_pincone_pdfdata(x[0],x[1])).\
    #                                             foreach(lambda x : upsert_pinecone_data(x))
                                                

    
    
    
if __name__ == "__main__":
    main()

    
