from pymongo import MongoClient, ASCENDING, DESCENDING

def get_mongodb_client():
    '''
    Fetches MongoDB Client
    '''
    return MongoClient("mongodb://localhost:27017/")

def write_data(df, collection):
    '''
    Writes dataframe in MongoDB using Spark connector
    '''
    try:             
        df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "gdelt") \
            .option("collection", f"{collection}") \
            .save()
        print(f"Succesfully saved data into MongoDB, collection {collection}")

    except Exception as e:
        print(f"Failed writing data into MongoDB: {e}")

def drop_collections(db, collections, all_collections = True):
    '''
    Drops GDELT collections in DB
    If all_collections = False, only drops analysis collections
    '''
    if all_collections:
        print("Dropping all old collections...")

        for collection in collections:
            db[collection].drop()
    else:
        print("Dropping old analysis collections...")
        
        for collection in collections:
            if collection != "events":
                db[collection].drop()

    print("Old collections succesfully dropped!")

def setup_indexes(db, collections):

    print("Setting up compound indexes for collections...")

    for collection in collections:

        # ── events / top_events / separate_events / top_negative_events / top_impact_events ──
        # Queried by: ActionGeo_CountryCode, event_date, lat, lon,
        #             goldstein_scale, avg_tone, num_mentions, num_articles,
        #             Actor1Name, Actor2Name, ActionGeo_FullName
        if collection in ("events", "top_events", "separate_events",
                          "top_negative_events", "top_impact_events"):

            # Country + date – most common compound filter in get_user_queried_events
            db[collection].create_index(
                [("ActionGeo_CountryCode", ASCENDING), ("event_date", ASCENDING)],
                name="country_date_idx",
                background=True,
            )

            # Full location name (regex filter in get_user_queried_events)
            db[collection].create_index(
                [("ActionGeo_FullName", ASCENDING)],
                name="geo_fullname_idx",
                background=True,
            )

            # Actor filters
            db[collection].create_index(
                [("Actor1Name", ASCENDING)],
                name="actor1_idx",
                background=True,
            )
            db[collection].create_index(
                [("Actor2Name", ASCENDING)],
                name="actor2_idx",
                background=True,
            )

            # Numeric range filters used together in queries
            db[collection].create_index(
                [("goldstein_scale", ASCENDING), ("avg_tone", ASCENDING), ("num_mentions", ASCENDING)],
                name="numeric_filters_idx",
                background=True,
            )

            # Sort field for results (num_articles DESC)
            db[collection].create_index(
                [("num_articles", DESCENDING)],
                name="num_articles_desc_idx",
                background=True,
            )

            # Geo coordinates (used in heatmap queries)
            db[collection].create_index(
                [("lat", ASCENDING), ("lon", ASCENDING)],
                name="lat_lon_idx",
                background=True,
            )

            # event_date alone – used to filter by distinct date in get_top_events
            db[collection].create_index(
                [("event_date", ASCENDING)],
                name="event_date_idx",
                background=True,
            )

        # ── events_per_country ──────────────────────────────────────────────
        # Queried via get_ui_data (simple find); sorted by Total_Events
        elif collection == "events_per_country":
            db[collection].create_index(
                [("Total_Events", DESCENDING)],
                name="total_events_desc_idx",
                background=True,
            )
            db[collection].create_index(
                [("Country_Name", ASCENDING)],
                name="country_name_idx",
                background=True,
            )

        # ── tone_by_country ─────────────────────────────────────────────────
        # Queried in get_tone_extremes: sort by Average_Tone ASC / DESC
        elif collection == "tone_by_country":
            db[collection].create_index(
                [("Average_Tone", DESCENDING)],
                name="tone_desc_idx",
                background=True,
            )
            db[collection].create_index(
                [("Country_Name", ASCENDING)],
                name="country_name_idx",
                background=True,
            )

        # ── country_event_spike ─────────────────────────────────────────────
        # Queried in get_country_event_spikes: filter is_spike, sort spike_score DESC
        elif collection == "country_event_spike":
            db[collection].create_index(
                [("ActionGeo_CountryCode", ASCENDING)],
                name="country_idx",
                background=True,
            )
            db[collection].create_index(
                [("is_spike", ASCENDING), ("spike_score", DESCENDING)],
                name="spike_filter_score_idx",
                background=True,
            )
            db[collection].create_index(
                [("spike_score", DESCENDING)],
                name="spike_score_desc_idx",
                background=True,
            )

    print("Finished setting up indexes!")
