import os
from pyspark.sql import SparkSession
from download_data import get_file_list, download_files
from pyspark.sql.functions import col, to_date, lit
from utils.file_utils import clear_data, unzip_files
from clean_data import ingest_data, data_cleaner

DATA_FOLDER = "data"
ZIP_DIR = "data/raw_zips"
EXTRACT_DIR = "data/extracted"

if os.path.isdir(DATA_FOLDER):
    clear_data(DATA_FOLDER)

def main():
    spark = SparkSession.builder.appName("GDELT-Analyzer").getOrCreate()
    url_list = get_file_list()
    if url_list:
        download_files(url_list)

    extracted = unzip_files(ZIP_DIR, EXTRACT_DIR)
    if not extracted:
        raise FileNotFoundError("No extracted GDELT files found in data/raw_unzipped")

    full_df = ingest_data(spark, extracted)
    clean_df = data_cleaner(full_df)


if __name__=="__main__":
    main()