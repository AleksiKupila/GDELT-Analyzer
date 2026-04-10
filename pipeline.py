import os
import time
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

def fetch_new_data(spark, time_limit):
     # Fetch CAMEO codes and the master URL list from GDELT
    event_codes = get_event_codes(EVENT_CODE_URL)
    url_list = get_file_list("export", MASTER_LIST_URL, time_limit)

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

def run_pipeline(time_limit=168, clear=False, download=False, analyze=False, index=False):

    _pipeline_start = time.perf_counter()
    print("\n=== Pipeline started ===")

    collections = [
        "events",
        "separate_events", 
        "top_negative_events", 
        "top_impact_events", 
        "top_events", 
        "events_per_country", 
        "tone_by_country", 
        "country_event_spike",
        "events_by_time_and_country"]

    # ── Connect to MongoDB ────────────────────────────────────────────────────
    _t = time.perf_counter()
    try:
        mongo_client = MongoClient(MONGO_CLIENT_URL)
        print(f"Succesfully connected to MongoDB client at {MONGO_CLIENT_URL}")
        db = mongo_client["gdelt"]
    except Exception as e:
        print(f"Connecting to MongoDB client failed: {e}")
    print(f"[timer] MongoDB connection: {time.perf_counter() - _t:.2f}s")

    # ── Start Spark session ───────────────────────────────────────────────────
    _t = time.perf_counter()
    try:
        spark = SparkSession.builder \
            .appName("GDELT-Analyzer") \
            .config("spark.mongodb.read.connection.uri", f"{MONGO_CLIENT_URL}db.collection") \
            .config("spark.mongodb.write.connection.uri", f"{MONGO_CLIENT_URL}db.collection") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.0.1") \
            .config("spark.executor.memoryOverhead", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "2g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        # Force UTC even if an existing session was reused via getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        print("Succesfully created Spark session!\n")
    except Exception as e:
        print(f"Failed to initialize Spark session: {e}")
    print(f"[timer] Spark session startup: {time.perf_counter() - _t:.2f}s")

    # ── Clear ─────────────────────────────────────────────────────────────────
    if clear:
        _t = time.perf_counter()
        drop_collections(db, collections, True)
        if os.path.isdir(DATA_DIR):
            clear_data(DATA_DIR)
        print(f"[timer] Clear step: {time.perf_counter() - _t:.2f}s")

    # ── Download & ingest ─────────────────────────────────────────────────────
    if download:
        _t = time.perf_counter()
        time_hours = time_limit
        clean_df, event_codes = fetch_new_data(spark, time_hours)
        print(f"[timer] Data download & extraction: {time.perf_counter() - _t:.2f}s")

        _t = time.perf_counter()
        event_codes_df = ingest_cameo_data(spark, event_codes)
        df_with_code_descriptions = join_cameo_df(event_codes_df, clean_df)
        write_data(df_with_code_descriptions, "events")
        print(f"[timer] CAMEO join & MongoDB write: {time.perf_counter() - _t:.2f}s")

    # ── Analysis ──────────────────────────────────────────────────────────────
    if analyze:
        _t = time.perf_counter()
        if download:
            analysis_df = df_with_code_descriptions
        else:
            drop_collections(db, collections, False)
            analysis_df = (
                spark.read
                .format("mongodb")
                .option("database", "gdelt")
                .option("collection", "events")
                .load()
            )
        print(f"[timer] Analysis DataFrame preparation: {time.perf_counter() - _t:.2f}s")

        _t = time.perf_counter()
        run_analysis(analysis_df, time_limit)
        print(f"[timer] run_analysis (all sub-tasks): {time.perf_counter() - _t:.2f}s")

    # ── Index setup ───────────────────────────────────────────────────────────
    if index:
        _t = time.perf_counter()
        setup_indexes(db, collections)
        print(f"[timer] Index setup: {time.perf_counter() - _t:.2f}s")

    _total = time.perf_counter() - _pipeline_start
    print(f"\n=== Pipeline completed successfully! Total elapsed: {_total:.2f}s ===\n")

if __name__=="__main__":
    run_pipeline()