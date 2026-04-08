from core.mongo_utils import get_mongodb_client
import pandas as pd

def get_ui_data(collection_name, limit=1000):

    db = get_mongodb_client()["gdelt"]
    collection = db[collection_name]
    
    # Fetch data and convert to Pandas DF
    cursor = collection.find().limit(limit)
    df = pd.DataFrame(list(cursor))
    
    # Clean up MongoDB internal ID for cleaner display
    if not df.empty and '_id' in df.columns:
        df.drop(columns=['_id'], inplace=True)
    
    return df

def get_tone_extremes(limit=10):
    db = get_mongodb_client()["gdelt"]
    
    pipeline = [
        {
            "$facet": {
                "most_positive": [
                    { "$sort": { "Average_Tone": -1 } },
                    { "$limit": limit },
                    { "$project": { "_id": 0, "Country_Name": 1, "Average_Tone": 1 } }
                ],
                "most_negative": [
                    { "$sort": { "Average_Tone": 1 } },
                    { "$limit": limit },
                    { "$project": { "_id": 0, "Country_Name": 1, "Average_Tone": 1 } }
                ]
            }
        }
    ]
    
    results = list(db.tone_by_country.aggregate(pipeline))[0]
    return results if results else {"most_positive": [], "most_negative": []}

def get_user_queried_events(country, actor1, actor2, date_min, date_max, goldstein_min, goldstein_max, tone_min, tone_max, limit=10):
    db = get_mongodb_client()["gdelt"]
    pipeline = [
        {
            "$match": {
                "ActionGeo_FullName": country,
                "Actor1Name": actor1,
                "Actor2Name": actor2,
                "event_date": {"$gte": date_min, "$lte": date_max},
                "goldstein_scale": {
                    "$gte": goldstein_min,
                    "$lte": goldstein_max
                },
                "avg_tone": {
                    "$gte": tone_min,
                    "$lte": tone_max
                }
            }
        },
        { "$sort": { "num_articles": -1 } },
        { "$limit": limit },
        {
            "$project": {
                "_id": 0,
                "EventDescription": 1,
                "event_date": 1,
                "Actor1Name": 1,
                "Actor2Name": 1,
                "num_mentions": 1,
                "num_articles": 1,
                "goldstein_scale": 1,
                "QuadClass": 1,
                "avg_tone": 1,
                "ActionGeo_FullName": 1,
                "lon": 1,
                "lat": 1,
                "SOURCEURL": 1
            }
        }
    ]
    results = list(db.events.aggregate(pipeline))
    df = pd.DataFrame(results if results else [])
    return df


def get_country_event_spikes(only_spikes=False, limit=50):
    """
    Retrieves pre-computed country event spike results from MongoDB.

    Parameters
    ----------
    only_spikes : bool
        When True only countries flagged as ``is_spike = True`` are returned.
        Defaults to False (return all countries, sorted by spike_score desc).
    limit : int
        Maximum number of rows to return.  Defaults to 50.

    Returns
    -------
    pandas.DataFrame
        Columns: ``ActionGeo_CountryCode``, ``spike_events``,
        ``baseline_events``, ``hourly_baseline_rate``,
        ``expected_spike_events``, ``spike_score``, ``is_spike``.
        Empty DataFrame if the collection does not exist yet.
    """
    db = get_mongodb_client()["gdelt"]
    collection = db["country_event_spike"]

    match_stage = {"$match": {"is_spike": True}} if only_spikes else {"$match": {}}

    pipeline = [
        match_stage,
        {"$sort": {"spike_score": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "ActionGeo_CountryCode": 1,
                "spike_events": 1,
                "baseline_events": 1,
                "hourly_baseline_rate": 1,
                "expected_spike_events": 1,
                "spike_score": 1,
                "is_spike": 1,
            }
        },
    ]

    results = list(collection.aggregate(pipeline))
    return pd.DataFrame(results if results else [])
