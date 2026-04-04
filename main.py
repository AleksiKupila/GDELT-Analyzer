import os
from pyspark.sql import SparkSession
from download_data import get_file_list, download_files, get_event_codes
from pyspark.sql.functions import col, to_date, lit
from utils.file_utils import clear_data, unzip_files
from ingest_clean import ingest_gkg_data, clean_event_data, ingest_event_data, ingest_cameo_data
from join_data import join_cameo_df
from mongodb import Mongo_client
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

    df_with_code_descriptions = join_cameo_df(event_codes_df, clean_df)

    try:             
        df_with_code_descriptions.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "gdelt") \
            .option("collection", "events") \
            .save()
        print("Succesfully saved data into MongoDB!")

    except Exception as e:
        print(f"Failed writing data into MongoDB: {e}")

    df_with_code_descriptions \
    .orderBy(col("num_mentions").desc()) \
    .select("event_date", "num_mentions", "Actor1Name", "Actor2Name", "ActionGeo_FullName", "EventDescription") \
    .show(10, truncate=False)
    
if __name__=="__main__":
    main()