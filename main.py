import os
from pyspark.sql import SparkSession
from download_data import get_file_list, download_files
from pyspark.sql.functions import col, to_date, lit
from utils.file_utils import clear_data, unzip_files
from clean_data import ingest_gkg_data, clean_extracted, ingest_event_data

DATA_DIR = "data"
ZIP_DIR = "data/raw_zips"
EXTRACT_DIR = "data/extracted"

if os.path.isdir(DATA_DIR):
    clear_data(DATA_DIR)

def main():
    spark = SparkSession.builder.appName("GDELT-Analyzer").getOrCreate()
    url_list = get_file_list("export")
    if url_list:
        download_files(url_list)

  
    extracted = unzip_files(ZIP_DIR, EXTRACT_DIR)
    if not extracted:
        raise FileNotFoundError("No extracted GDELT files found in data/raw_unzipped")

    #full_df = ingest_gkg_data(spark, extracted)
    event_df = ingest_event_data(spark, extracted)
    clean_df = clean_extracted(event_df)
    """
    full_df \
    .orderBy(col("DATE").desc()) \
    .select("DATE", "SOURCECOMMONNAME", "V1THEMES", "V1LOCATIONS") \
    .show(10)
    """
    
if __name__=="__main__":
    main()