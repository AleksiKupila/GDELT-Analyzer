import os
from pyspark.sql import SparkSession
from download_data import get_file_list, download_files, get_event_codes
from pyspark.sql.functions import col, to_date, lit
from utils.file_utils import clear_data, unzip_files
from ingest_clean import ingest_gkg_data, clean_event_data, ingest_event_data, ingest_cameo_data
from join_data import join_cameo_df
from mongodb import Mongo_client

DATA_DIR = "data"
ZIP_DIR = "data/raw_zips"
EXTRACT_DIR = "data/extracted"
MONGO_CLIENT_URL = "mongodb://127.0.0.1:27017/"

spark = SparkSession.builder.appName("GDELT-Analyzer").getOrCreate()
mongodb = Mongo_client(MONGO_CLIENT_URL)

if os.path.isdir(DATA_DIR):
    clear_data(DATA_DIR)

mongodb.drop_collections(["events"])

def main():
    event_codes = get_event_codes()
    url_list = get_file_list("export")

    if url_list:
        download_files(url_list)

    mongodb.new_db("events")

    extracted = unzip_files(ZIP_DIR, EXTRACT_DIR)
    if not extracted:
        raise FileNotFoundError("No extracted GDELT files found in data/raw_unzipped")

    #full_df = ingest_gkg_data(spark, extracted)
    event_df = ingest_event_data(spark, extracted)
    clean_df = clean_event_data(event_df)
    event_codes_df = ingest_cameo_data(spark, event_codes)

    df_with_code_descriptions = join_cameo_df(event_codes_df, clean_df)

    df_with_code_descriptions \
    .orderBy(col("num_mentions").desc()) \
    .select("event_date", "num_mentions", "Actor1Name", "Actor2Name", "ActionGeo_FullName", "EventDescription") \
    .show(10, truncate=False)
    
if __name__=="__main__":
    main()