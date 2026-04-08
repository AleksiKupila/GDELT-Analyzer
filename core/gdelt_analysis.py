from pyspark.sql.functions import *
from pyspark.sql.functions import round as spark_round
from pyspark import StorageLevel
from core.mongo_utils import write_data
from pyspark.sql.window import Window

def join_cameo_df(cameo_df, event_df):
    return event_df.join(cameo_df, on="EventCode", how="inner")

def separate_events(df):
    '''
    Function that attempts to group events by country, date, quad class and event codes.
    Includes sample URLs for each event for later AI analysis.
    '''
    url_window = Window.partitionBy(
        "ActionGeo_CountryCode", "event_date", "QuadClass", "EventBaseCode"
    ).orderBy(col("num_mentions").desc())

    ranked = df \
        .filter(col("ActionGeo_CountryCode").isNotNull()) \
        .withColumn("url_rank", row_number().over(url_window))

    # Collect up to 20 top URLs per group using a conditional collect_list on pre-ranked rows
    result = ranked \
        .groupBy(
            "ActionGeo_CountryCode",
            "event_date",
            "QuadClass",
            "EventBaseCode"
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
            # Only collect SOURCEURL for the top-20 ranked rows — avoids a
            # full collect_list + in-memory sort on every group
            collect_list(
                when(col("url_rank") <= 20, col("SOURCEURL"))
            ).alias("sample_urls")
        ).filter("total_sources > 20") \
        .orderBy(col("total_articles").desc())

    write_data(result, "separate_events")

          
def impactful_events(df):
    result = df.withColumn(
        "impact_score",
        col("avg_goldstein_scale") * col("total_articles")) \
        .orderBy(col("impact_score").asc())
    write_data(result, "top_impact_events")

def top_events(df):
    '''
    Returns top 1000 events by article count.
    Filters out events with null coordinate values.
    '''
    result = df.filter(col("lat").isNotNull()) \
        .filter(col("lon").isNotNull()) \
        .orderBy(col("num_articles").desc()) \
        .limit(1000)
    
    write_data(result, "top_events")

def events_by_country(df):
    '''
    Groups events by country, aggregates total events.
    Excludes rows where ActionGeo_CountryCode is null.
    '''
    result = df.filter(col("ActionGeo_CountryCode").isNotNull()) \
        .groupBy("ActionGeo_CountryCode")  \
        .agg(
            count("*").alias("Total_Events"),
            mode("ActionGeo_FullName").alias("Country_Name")) \
        .select(
            "Country_Name", 
            "Total_Events"
        ).orderBy(col("Total_Events").desc())
    
    write_data(result, "events_per_country")

def tone_by_country(df):
    result = df.groupBy("ActionGeo_CountryCode") \
        .agg(
            avg("avg_tone").alias("Average_Tone"),
            mode("ActionGeo_FullName").alias("Country_Name")
        ).select("Country_Name", "Average_Tone") \
        .orderBy(col("Average_Tone").desc())

    write_data(result, "tone_by_country")



def country_event_spike(df, timestamp_col="event_date", baseline_hours=144, spike_hours=24, spike_threshold=0.5):
    """
    Detects countries experiencing an unusual spike in event activity.
    Works best with longer baselines, such as 144h/6 days.


    """
    events_by_time = (
        df
        .filter(col("ActionGeo_CountryCode").isNotNull())
        .groupBy("ActionGeo_CountryCode", timestamp_col)
        .agg(count("*").alias("total_events"))
        .withColumn("event_ts", to_timestamp(col(timestamp_col)))
        .filter(col("event_ts").isNotNull())
    )

    # Get latest global timestamp (newest event)
    latest_ts = events_by_time.agg(max("event_ts").alias("global_latest_ts")).first()["global_latest_ts"]

    # Calculate how far the event is from the newest event
    # Then label the event to be in spike or baseline period depending on its age
    labelled = (
        events_by_time
        .withColumn(
            "hours_from_latest",
            (unix_timestamp(lit(latest_ts)) - unix_timestamp("event_ts")) / lit(3600.0)
        )
        .withColumn(
            "is_spike_period",
            (col("hours_from_latest") >= 0) & (col("hours_from_latest") < lit(float(spike_hours)))
        )
        .withColumn(
            "is_baseline_period",
            (col("hours_from_latest") >= lit(float(spike_hours))) & (col("hours_from_latest") <= lit(float(spike_hours + baseline_hours)))
        )
    )

    # Group by country and calculate total events that happened at the same time
    aggregated = (
        labelled
        .groupBy("ActionGeo_CountryCode")
        .agg(
            sum(when(col("is_spike_period"),    col("total_events")).otherwise(lit(0))).alias("spike_events"),
            sum(when(col("is_baseline_period"), col("total_events")).otherwise(lit(0))).alias("baseline_events"),
        )
    )

    # Calculate baseline (the average events/hour)
    # Then Calculate what is expected for the spike period to be 'normal' amount of events
    with_expected = (
        aggregated
        .withColumn(
            "hourly_baseline_rate",
            spark_round(col("baseline_events") / lit(float(baseline_hours)), 4)
        )
        .withColumn(
            "expected_spike_events",
            spark_round(col("hourly_baseline_rate") * lit(float(spike_hours)), 4)
        )
    )

    # Calculate score based on expected vs actual spike events
    scored = (
        with_expected
        .withColumn(
            "spike_score",
            when(
                col("expected_spike_events") > 0,
                spark_round(
                    (col("spike_events") - col("expected_spike_events")) / col("expected_spike_events"),
                    4
                )
            ).otherwise(
                when(col("spike_events") > 0, col("spike_events").cast("double"))
                .otherwise(lit(0.0))
            )
        )
        .withColumn("is_spike", col("spike_score") > lit(spike_threshold))
        .filter("baseline_events > 200")
        .orderBy(col("spike_score").desc())
    )

    write_data(scored, "country_event_spike")


def run_analysis(df_with_code_descriptions):

    cached_df = df_with_code_descriptions.persist(StorageLevel.DISK_ONLY)
    try:
        # Top events worldwide by article count
        top_events(cached_df)

        # Total events per country
        events_by_country(cached_df)

        # DF that separates events by location and topic
        separate_events(cached_df)

        # Average_tone by country
        tone_by_country(cached_df)

        country_event_spike(cached_df)
        
    finally:
        cached_df.unpersist()
