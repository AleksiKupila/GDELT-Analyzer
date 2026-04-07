import os
from pyspark.sql import SparkSession
from utils.download_utils import *
from utils.file_utils import *
from core.mongo_utils import *
from core.gdelt_analysis import *
from core.spark_ingest import *
from core.mongo_utils import *
from pymongo import MongoClient

# Directory names, MongoDB client URL
DATA_DIR = "data"
ZIP_DIR = "data/raw_zips"
EXTRACT_DIR = "data/extracted"
MONGO_CLIENT_URL = "mongodb://127.0.0.1:27017/"

# Downloads related
MASTER_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
EVENT_CODE_URL = "https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt"
TIME_LIMIT = 8


def fetch_new_data(spark):
     # Fetch CAMEO codes and the master URL list from GDELT
    event_codes = get_event_codes(EVENT_CODE_URL)
    url_list = get_file_list("export", MASTER_LIST_URL, TIME_LIMIT)

    # Download files from GDELT
    if url_list:
        download_files(url_list)

    # Unzip downloaded files
    extracted = unzip_files(ZIP_DIR, EXTRACT_DIR)
    if not extracted:
        raise FileNotFoundError("No extracted GDELT files found in data/raw_unzipped")

    # Ingest event data and clean
    event_df = ingest_event_data(spark, extracted)
    clean_df = clean_event_data(event_df)

    return clean_df, event_codes

def run_pipeline():
    
    # Connect to MongoDB Client
    try:
        mongo_client = MongoClient(MONGO_CLIENT_URL)
        print(f"Succesfully connected to MongoDB client at {MONGO_CLIENT_URL}")
    except Exception as e:
        print(f"Connecting to MongoDB client failed: {e}")

    # Start Spark session
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
    drop_collections(db)

    # Clear old files
    if os.path.isdir(DATA_DIR):
        clear_data(DATA_DIR)

    clean_df, event_codes = fetch_new_data(spark)

    # Join GDELT CAMEO codes to the DF
    event_codes_df = ingest_cameo_data(spark, event_codes)
    df_with_code_descriptions = join_cameo_df(event_codes_df, clean_df)
    write_data(df_with_code_descriptions, "events")

    run_analysis(df_with_code_descriptions)
    print("Pipeline ran succesfully!")

if __name__=="__main__":
    run_pipeline()