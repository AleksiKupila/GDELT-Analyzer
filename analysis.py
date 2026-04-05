from pyspark.sql.functions import *

def join_cameo_df(cameo_df, event_df):
    return event_df.join(cameo_df, on="EventCode", how="inner")

def separate_events(df):

    meaningful_events = ["05", "06", "07", "08", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"]
    #df = df.dropDuplicates(["SOURCEURL"])

    separate_events = df \
        .groupBy(
            "ActionGeo_CountryCode",
            "event_date",
            "QuadClass",
            "EventBaseCode",
            "EventRootCode" \
        ).agg( 
            count("*").alias("total_events"),
            sum("num_mentions").alias("total_mentions"),
            sum("num_articles").alias("total_articles"),
            sum("num_sources").alias("total_sources"),
            avg("avg_tone").alias("average_tone"),
            avg("goldstein_scale").alias("avg_goldstein_scale"),
            mode("ActionGeo_FullName").alias("full_location"),
            mode("Actor1Name").alias("actor_1_name"),
            mode("Actor2Name").alias("actor_2_name"),
            mode("ActionGeo_Lat").alias("lat_coordinates"),
            mode("ActionGeo_Long").alias("long_coordinates"),
            mode("EventDescription").alias("event_description"),
            array_distinct(collect_list("SOURCEURL")).alias("all_urls")
        ).filter("total_sources > 30") \
        .filter(col("ActionGeo_CountryCode").isNotNull()) \
        .filter(col("EventRootCode").isin(meaningful_events)) \
        .orderBy(col("avg_goldstein_scale").asc())
    '''
    separate_events = separate_events.withColumn(
        "dissonance_score",
        abs(col("avg_goldstein_scale") - (col("average_tone") / lit(10)))
    )
    '''
    return separate_events
    

        
def top_events(df):
    top_events = df \
        .orderBy(col("num_mentions").desc()) \
        .select("event_date", "num_mentions", "Actor1Name", "Actor2Name", "ActionGeo_FullName", "EventDescription") \
        .limit(20)
    return top_events