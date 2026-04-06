from pyspark.sql.functions import *

MEANINGFUL_EVENTS = ["05", "06", "07", "08", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"]


def join_cameo_df(cameo_df, event_df):
    return event_df.join(cameo_df, on="EventCode", how="inner")

def separate_events(df):

    separate_events = df \
        .groupBy(
            "ActionGeo_CountryCode",
            "event_date",
            "QuadClass",
            "EventBaseCode",
            "EventRootCode"
        ).agg(
            count("*").alias("total_events"),
            sum("num_mentions").alias("total_mentions"),
            sum("num_articles").alias("total_articles"),
            sum("num_sources").alias("total_sources"),
            avg("avg_tone").alias("average_tone"),
            avg("goldstein_scale").alias("avg_goldstein_scale"),
            mode("ActionGeo_FullName").alias("top_location"),
            mode("Actor1Name").alias("top_actor_1_name"),
            mode("Actor2Name").alias("top_actor_2_name"),
            mode("lon"),
            mode("lat"),
            mode("EventDescription").alias("top_event_description"),
            slice(
                transform(
                    sort_array(
                        array_distinct(
                            collect_list(
                                struct(
                                    col("num_mentions").alias("mentions"),
                                    col("SOURCEURL").alias("url"),
                                )
                            ),
                        ),
                        asc=False
                    ),
                    lambda x: x["url"]
                ),
                1, 20
            ).alias("sample_urls")
        ).filter("total_sources > 20") \
        .filter(col("ActionGeo_CountryCode").isNotNull()) \
        .filter(col("EventRootCode").isin(MEANINGFUL_EVENTS)) \
        .orderBy(col("total_articles").desc()) \

    return separate_events
          
def negative_events(df):

    top_events = df \
        .orderBy(col("average_tone").asc()) \
        .limit(15)
    
    return top_events

def impactful_events(df):

    df = df.withColumn(
        "impact_score",
        col("avg_goldstein_scale") * col("total_articles")) \
        .orderBy(col("impact_score").asc()) \
        .limit(15)
    return df

def top_events(df):
    df = df \
        .filter(col("lat").isNotNull()) \
        .filter(col("lon").isNotNull()) \
        .orderBy(col("num_articles").desc()) \
        .limit(2000)
        
    return df