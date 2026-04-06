import os
from pyspark.sql import SparkSession
from utils.download_data import *
from utils.file_utils import clear_data, unzip_files
from ingest_clean import *
from analysis import *
from mongodb import write_data
from pymongo import MongoClient

DATA_DIR = "data"
ZIP_DIR = "data/raw_zips"
EXTRACT_DIR = "data/extracted"
MONGO_CLIENT_URL = "mongodb://127.0.0.1:27017/"

try:
    mongo_client = MongoClient(MONGO_CLIENT_URL)
    print(f"Succesfully connected to MongoDB client at {MONGO_CLIENT_URL}")
except Exception as e:
    print(f"Connecting to MongoDB client failed: {e}")

try:
    spark = SparkSession.builder \
        .appName("GDELT-Analyzer") \
        .config("spark.mongodb.read.connection.uri", f"{MONGO_CLIENT_URL}db.collection") \
        .config("spark.mongodb.write.connection.uri", f"{MONGO_CLIENT_URL}db.collection") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.0.1") \
        .getOrCreate()
    print("Succesfully created Spark session!\n")
except Exception as e:
    print(f"Failed to initialize Spark session: {e}")

db = mongo_client["gdelt"]
print("Dropping old collections...")
db["events"].drop()
db["separate_events"].drop()
db["top_negative_events"].drop()
db["top_impact_events"].drop()
db["top_impact_event_locations"].drop()

if os.path.isdir(DATA_DIR):
    clear_data(DATA_DIR)

def main():
    event_codes = get_event_codes()
    url_list = get_file_list("export")

    if url_list:
        download_files(url_list)

    extracted = unzip_files(ZIP_DIR, EXTRACT_DIR)
    if not extracted:
        raise FileNotFoundError("No extracted GDELT files found in data/raw_unzipped")

    #full_df = ingest_gkg_data(spark, extracted)
    event_df = ingest_event_data(spark, extracted)
    clean_df = clean_event_data(event_df)
    event_codes_df = ingest_cameo_data(spark, event_codes)

    # Join GDELT CAMEO codes to the DF
    df_with_code_descriptions = join_cameo_df(event_codes_df, clean_df)
    write_data(df_with_code_descriptions, "events")
    '''
    # DF that separates events by location and topic
    separate_events_df = separate_events(df_with_code_descriptions)
    write_data(separate_events_df, "separate_events")

    # Events with most negative tone
    top_negative_events = negative_events(separate_events_df)
    write_data(top_negative_events, "top_negative_events")

    # Events with most theoretical impact
    top_impact_events_df = impactful_events(separate_events_df)
    top_impact_locations = event_locations(top_impact_events_df)
    write_data(top_impact_events_df, "top_impact_events")
    write_data(top_impact_locations, "top_impact_event_locations")
'''
if __name__=="__main__":
    main()