import glob
import os
import zipfile

from utils.file_utils import unzip_files
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date

def ingest_data(spark, data_files):

    schema = StructType([
        StructField("GLOBALEVENTID", LongType(), True),
        StructField("SQLDATE", IntegerType(), True),
        StructField("MonthYear", IntegerType(), True),
        StructField("Year", IntegerType(), True),
        StructField("FractionDate", DoubleType(), True),

        StructField("Actor1Code", StringType(), True),
        StructField("Actor1Name", StringType(), True),
        StructField("Actor1CountryCode", StringType(), True),
        StructField("Actor1KnownGroupCode", StringType(), True),
        StructField("Actor1EthnicCode", StringType(), True),
        StructField("Actor1Religion1Code", StringType(), True),
        StructField("Actor1Religion2Code", StringType(), True),
        StructField("Actor1Type1Code", StringType(), True),
        StructField("Actor1Type2Code", StringType(), True),
        StructField("Actor1Type3Code", StringType(), True),

        StructField("Actor2Code", StringType(), True),
        StructField("Actor2Name", StringType(), True),
        StructField("Actor2CountryCode", StringType(), True),
        StructField("Actor2KnownGroupCode", StringType(), True),
        StructField("Actor2EthnicCode", StringType(), True),
        StructField("Actor2Religion1Code", StringType(), True),
        StructField("Actor2Religion2Code", StringType(), True),
        StructField("Actor2Type1Code", StringType(), True),
        StructField("Actor2Type2Code", StringType(), True),
        StructField("Actor2Type3Code", StringType(), True),

        StructField("IsRootEvent", IntegerType(), True),
        StructField("EventCode", StringType(), True),
        StructField("EventBaseCode", StringType(), True),
        StructField("EventRootCode", StringType(), True),
        StructField("QuadClass", IntegerType(), True),

        StructField("GoldsteinScale", DoubleType(), True),
        StructField("NumMentions", IntegerType(), True),
        StructField("NumSources", IntegerType(), True),
        StructField("NumArticles", IntegerType(), True),
        StructField("AvgTone", DoubleType(), True),

        StructField("Actor1Geo_Type", IntegerType(), True),
        StructField("Actor1Geo_FullName", StringType(), True),
        StructField("Actor1Geo_CountryCode", StringType(), True),
        StructField("Actor1Geo_ADM1Code", StringType(), True),
        StructField("Actor1Geo_ADM2Code", StringType(), True),
        StructField("Actor1Geo_Lat", DoubleType(), True),
        StructField("Actor1Geo_Long", DoubleType(), True),
        StructField("Actor1Geo_FeatureID", StringType(), True),

        StructField("Actor2Geo_Type", IntegerType(), True),
        StructField("Actor2Geo_FullName", StringType(), True),
        StructField("Actor2Geo_CountryCode", StringType(), True),
        StructField("Actor2Geo_ADM1Code", StringType(), True),
        StructField("Actor2Geo_ADM2Code", StringType(), True),
        StructField("Actor2Geo_Lat", DoubleType(), True),
        StructField("Actor2Geo_Long", DoubleType(), True),
        StructField("Actor2Geo_FeatureID", StringType(), True),

        StructField("ActionGeo_Type", IntegerType(), True),
        StructField("ActionGeo_FullName", StringType(), True),
        StructField("ActionGeo_CountryCode", StringType(), True),
        StructField("ActionGeo_ADM1Code", StringType(), True),
        StructField("ActionGeo_ADM2Code", StringType(), True),
        StructField("ActionGeo_Lat", DoubleType(), True),
        StructField("ActionGeo_Long", DoubleType(), True),
        StructField("ActionGeo_FeatureID", StringType(), True),

        StructField("DATEADDED", LongType(), True),
        StructField("SOURCEURL", StringType(), True),
    ])

    raw_df = spark.read \
        .option("delimiter", "\t") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .csv(data_files, schema=schema)
    
    print(f"Total rows loaded: {raw_df.count()}")
    return raw_df


def data_cleaner(df):

    print("Cleaning ingested data...")
    clean_df = df \
    .withColumn("event_date", to_date(col("SQLDATE").cast("string"), "yyyyMMdd")) \
    .withColumn("year_month", col("MonthYear").cast("string")) \
    .withColumn("goldstein_scale", col("GoldsteinScale").cast("float")) \
    .withColumn("avg_tone", col("AvgTone").cast("float")) \
    .withColumn("num_mentions", col("NumMentions").cast("int")) \
    .withColumn("num_articles", col("NumArticles").cast("int")) \
    .withColumn("is_root_event", col("IsRootEvent").cast("boolean")) \
    .withColumn("date_added", to_date(col("DATEADDED").cast("string").substr(1,8), "yyyyMMdd")) \
    .select(

        "GLOBALEVENTID",
        "event_date",
        "year_month",
        "Actor1Name", "Actor1CountryCode", "Actor1Type1Code",
        "Actor2Name", "Actor2CountryCode",
        "EventCode", "EventBaseCode", "EventRootCode",
        "QuadClass", "goldstein_scale", "avg_tone",
        "num_mentions", "num_articles",
        "ActionGeo_FullName", "ActionGeo_CountryCode",
        "ActionGeo_Lat", "ActionGeo_Long",
        "SOURCEURL"
    ) \
    .filter(col("event_date").isNotNull())

    print(f"Total rows cleaned: {clean_df.count()}")
    return(clean_df)
    

