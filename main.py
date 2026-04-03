import os
from pyspark.sql import SparkSession
from download_data import get_file_list, download_files
from utils.file_utils import clear_data
from clean_data import ingest_data

DATA_FOLDER = "data"

if os.path.isdir(DATA_FOLDER):
    clear_data(DATA_FOLDER)

def main():
    spark = SparkSession.builder.appName("GDELT-Analyzer").getOrCreate()
    url_list = get_file_list()
    if url_list:
        download_files(url_list)
    
    ingest_data(spark)

if __name__=="__main__":
    main()