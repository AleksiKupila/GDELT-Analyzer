from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date

def ingest_gkg_data(spark, data_files):

    gkg_schema = StructType([
    # Core Identifiers
    StructField("GKGRECORDID", StringType(), True),                # Unique serial ID [cite: 459]
    StructField("DATE", StringType(), True),                   # YYYYMMDDHHMMSS format 
    StructField("SOURCECOLLECTIONIDENTIFIER", IntegerType(), True), # 1=Web, 2=Citation, etc. [cite: 473, 477]
    StructField("SOURCECOMMONNAME", StringType(), True),         # e.g., bbc.co.uk [cite: 484, 486]
    StructField("DOCUMENTIDENTIFIER", StringType(), True),       # URL or DOI [cite: 489, 492]
    
    # Counts & Themes
    StructField("OLDCOUNTS", StringType(), True),                   # Legacy semicolon-delimited counts [cite: 495]
    StructField("NEWCOUNTS", StringType(), True),                 # Counts with character offsets [cite: 516]
    StructField("V1THEMES", StringType(), True),                   # List of themes [cite: 520]
    StructField("V2ENHANCEDTHEMES", StringType(), True),           # Themes with offsets [cite: 524]
    
    # Locations
    StructField("V1LOCATIONS", StringType(), True),                # Semicolon-delimited locations [cite: 529]
    StructField("V2ENHANCEDLOCATIONS", StringType(), True),        # Locations with offsets and ADM2 [cite: 567]
    
    # Persons & Organizations
    StructField("V1PERSONS", StringType(), True),                  # List of persons [cite: 571]
    StructField("V2ENHANCEDPERSONS", StringType(), True),          # Persons with offsets [cite: 574]
    StructField("V1ORGANIZATIONS", StringType(), True),            # List of organizations [cite: 577]
    StructField("V2ENHANCEDORGANIZATIONS", StringType(), True),    # Organizations with offsets [cite: 587]
    
    # Tone & Dates
    StructField("V1.5TONE", StringType(), True),                   # 7 core emotional dimensions [cite: 590, 592]
    StructField("V2.1ENHANCEDDATES", StringType(), True),          # Date mentions with offsets [cite: 610]
    StructField("V2GCAM", StringType(), True),                     # 2,300+ emotional dimensions [cite: 380, 622]
    
    # Media & Social Embeds
    StructField("V2.1SHARINGIMAGE", StringType(), True),           # Primary article image URL [cite: 642, 643]
    StructField("V2.1RELATEDIMAGES", StringType(), True),          # Inline image URLs [cite: 646, 649]
    StructField("V2.1SOCIALIMAGEEMBEDS", StringType(), True),      # Twitter/Instagram image URLs [cite: 652, 653]
    StructField("V2.1SOCIALVIDEOEMBEDS", StringType(), True),      # YouTube/Vimeo/Vine URLs [cite: 657, 659]
    
    # Textual Analysis
    StructField("V2.1QUOTATIONS", StringType(), True),              # Quoted statements [cite: 661, 663]
    StructField("V2.1ALLNAMES", StringType(), True),               # All proper names (events, laws, etc) [cite: 670, 672]
    StructField("V2.1AMOUNTS", StringType(), True),                # Numeric amounts (troops, dollars, etc) [cite: 676, 678]
    
    # Translation & Extras
    StructField("V2.1TRANSLATIONINFO", StringType(), True),        # Source language and engine info [cite: 685]
    StructField("V2EXTRASXML", StringType(), True)                 # XML block for specialized data [cite: 695]
])

    df = spark.read \
        .option("delimiter", "\t") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .csv(data_files, schema=gkg_schema)
    
    print(f"Total rows loaded: {df.count()}")

    return df

def ingest_event_data(spark, data_files):

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

def clean_extracted(df):

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
    

