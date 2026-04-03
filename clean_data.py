from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

def ingest_data(spark):

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
        .csv("data/raw_zips/*.zip", schema=schema)
    
    print(f"Total rows loaded: {raw_df.count()}")