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
        .limit(10000)
    
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


def country_event_spike(df, timestamp_col="event_date", baseline_days=12, spike_threshold=0.5):
    """
    Detects countries experiencing an unusual spike in event activity.

    Since GDELT data is bucketed into daily timestamps and files are released
    every 15 minutes, the **latest day bucket is always partially filled**
    (only a few hours of events so far). To avoid comparing a full baseline day
    against an incomplete current day, the function skips the latest bucket
    and uses the **second-most-recent** bucket as the spike period:

      Global day buckets ranked newest → oldest:
        rank 1  — latest (partial/still-accumulating)  → SKIPPED
        rank 2  — most recently completed day           → SPIKE period
        rank 3…(2+baseline_days) — preceding days      → BASELINE period

    For every country the function computes:

    - ``spike_events``        — event count on the most recently completed day.
    - ``baseline_events``     — total events across ``baseline_days`` preceding days.
    - ``baseline_days_count`` — baseline day buckets actually present for that
                                country (may be < baseline_days for sparse data).
    - ``avg_baseline_per_day``— baseline_events / baseline_days_count.
    - ``spike_score``         — relative deviation from the daily average:
          spike_score = (spike_events - avg_baseline_per_day) / avg_baseline_per_day

      0 = normal activity; 1.0 = twice the daily average; negative = quieter.
      When baseline is zero but the spike day has events, spike_events is used
      directly so brand-new activity in previously quiet countries is surfaced.

    - ``is_spike`` — True when spike_score > spike_threshold (default 0.5).

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Raw or enriched GDELT event DataFrame containing at least
        ``ActionGeo_CountryCode`` and the column named by ``timestamp_col``.
    timestamp_col : str
        Name of the date/timestamp column.  Defaults to ``"event_date"``.
    baseline_days : int
        Number of completed day buckets to use as the baseline.
        Defaults to 6 (six full days preceding the spike day).
    spike_threshold : float
        Minimum spike_score required to set ``is_spike = True``.
        Defaults to 0.5.

    Returns
    -------
    None  (writes directly to the ``country_event_spike`` MongoDB collection)
    """

    # ------------------------------------------------------------------
    # 1. Count raw events per (country, day bucket)
    # ------------------------------------------------------------------
    events_by_day = (
        df
        .filter(col("ActionGeo_CountryCode").isNotNull())
        .groupBy("ActionGeo_CountryCode", timestamp_col)
        .agg(count("*").alias("total_events"))
        .withColumn("event_ts", col(timestamp_col).cast("timestamp"))
        .filter(col("event_ts").isNotNull())
    )

    # ------------------------------------------------------------------
    # 2. Rank all distinct global day buckets newest → oldest.
    #    rank 1 = latest (partially accumulated, skip)
    #    rank 2 = spike day (most recently *completed* day)
    #    rank 3…(2+baseline_days) = baseline days
    # ------------------------------------------------------------------
    day_rank_window = Window.orderBy(col("event_ts").desc())
    global_day_ranks = (
        events_by_day
        .select("event_ts")
        .distinct()
        .withColumn("day_rank", dense_rank().over(day_rank_window))
    )
    print("Global day bucket ranks (newest first):")
    global_day_ranks.orderBy("day_rank").show(10, truncate=False)

    # Join the rank back onto per-(country, day) rows
    events_ranked = events_by_day.join(global_day_ranks, on="event_ts", how="left")

    # ------------------------------------------------------------------
    # 3. Label rows by window membership using the rank
    # ------------------------------------------------------------------
    labelled = (
        events_ranked
        .withColumn(
            "is_spike_period",
            col("day_rank") == lit(2)
        )
        .withColumn(
            "is_baseline_period",
            (col("day_rank") >= lit(3)) & (col("day_rank") <= lit(2 + baseline_days))
        )
    )

    # ------------------------------------------------------------------
    # 4. Aggregate per country: spike day total and baseline totals
    # ------------------------------------------------------------------
    aggregated = (
        labelled
        .groupBy("ActionGeo_CountryCode")
        .agg(
            sum(when(col("is_spike_period"),    col("total_events")).otherwise(lit(0))).alias("spike_events"),
            sum(when(col("is_baseline_period"), col("total_events")).otherwise(lit(0))).alias("baseline_events"),
            sum(when(col("is_baseline_period"), lit(1)).otherwise(lit(0))).alias("baseline_days_count"),
        )
    )

    # ------------------------------------------------------------------
    # 5. Compute per-day baseline average and spike score
    # ------------------------------------------------------------------
    with_expected = (
        aggregated
        .withColumn(
            "avg_baseline_per_day",
            spark_round(
                when(col("baseline_days_count") > 0,
                     col("baseline_events") / col("baseline_days_count").cast("double"))
                .otherwise(lit(0.0)),
                4
            )
        )
    )

    scored = (
        with_expected
        .withColumn(
            "spike_score",
            when(
                col("avg_baseline_per_day") > 0,
                spark_round(
                    (col("spike_events") - col("avg_baseline_per_day")) / col("avg_baseline_per_day"),
                    4
                )
            ).otherwise(
                when(col("spike_events") > 0, col("spike_events").cast("double"))
                .otherwise(lit(0.0))
            )
        )
        .withColumn("is_spike", col("spike_score") > lit(spike_threshold))
        .filter("avg_baseline_per_day > 200")
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
        # separate_events(cached_df)

        # Average_tone by country
        tone_by_country(cached_df)

        country_event_spike(cached_df)

        # TESTING
        events_by_time_and_country(cached_df)
        
    finally:
        cached_df.unpersist()

# TESTING
def events_by_time_and_country(df):
    events_by_time = (
        df
        .filter(col("ActionGeo_CountryCode").isNotNull())
        .groupBy("ActionGeo_CountryCode", "event_date")
        .agg(count("*").alias("total_events"))
        .withColumn("event_ts", to_timestamp(col("event_date")))
        .filter(col("event_ts").isNotNull())
        .orderBy(col("event_ts").desc())
    )

    write_data(events_by_time, "events_by_time_and_country")
