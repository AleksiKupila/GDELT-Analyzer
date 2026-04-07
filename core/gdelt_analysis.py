from pyspark.sql.functions import *
from core.mongo_utils import write_data

MEANINGFUL_EVENTS = ["05", "06", "07", "08", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"]


def join_cameo_df(cameo_df, event_df):
    return event_df.join(cameo_df, on="EventCode", how="inner")

def separate_events(df):
    '''
    Function that attempts to group events by country, date, quad class and event codes.
    Includes sample URLs for each event for later AI analysis
    Does not count events with vague root codes
    Sorts events by article count
    '''
    separate_events = df \
        .filter(col("ActionGeo_CountryCode").isNotNull()) \
        .filter(col("EventRootCode").isin(MEANINGFUL_EVENTS)) \
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
    '''
    Returns top 2000 events by article count
    Filters out events with null coordinate values
    '''
    df = df \
        .filter(col("lat").isNotNull()) \
        .filter(col("lon").isNotNull()) \
        .orderBy(col("num_articles").desc()) \
        .limit(2000)
        
    return df

def events_by_country(df):
    '''
    Groups events by country, aggregates total events.
    Excludes rows where ActionGeo_CountryCode is null.
    '''
    return (
        df
        .filter(col("ActionGeo_CountryCode").isNotNull())
        .groupBy("ActionGeo_CountryCode")
        .agg(count("*").alias("total_events"))
        .select("ActionGeo_CountryCode", "total_events")
        .orderBy(col("total_events").desc())
    )

def run_analysis(df_with_code_descriptions):
    # Top events worldwide by article count
    top_events_df = top_events(df_with_code_descriptions)
    write_data(top_events_df, "top_events")

    # Total events per country
    events_per_country = events_by_country(df_with_code_descriptions)
    write_data(events_per_country, "events_per_country")

    # DF that separates events by location and topic
    separate_events_df = separate_events(df_with_code_descriptions)
    write_data(separate_events_df, "separate_events")

    # Events with most negative tone
    top_negative_events = negative_events(separate_events_df)
    write_data(top_negative_events, "top_negative_events")

    # Events with most theoretical impact
    top_impact_events_df = impactful_events(separate_events_df)
    write_data(top_impact_events_df, "top_impact_events")