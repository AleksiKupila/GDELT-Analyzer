from core.mongo_utils import get_mongodb_client
from datetime import datetime
import pandas as pd


def get_ui_data(collection_name, limit = 1000):
    """
    Fetch a pre-computed collection from MongoDB and return it as a DataFrame.
    """
    db = get_mongodb_client()["gdelt"]
    cursor = db[collection_name].find().limit(limit)
    df = pd.DataFrame(list(cursor))

    if not df.empty and "_id" in df.columns:
        df.drop(columns=["_id"], inplace=True)

    return df

def get_top_events(time = datetime(year=2026, month=4, day=8, hour=21, minute=0, second=0), limit = 1000):

    db = get_mongodb_client()["gdelt"]

    pipeline = [
        {"$match": {"event_date": time}},
        {"$project": {"_id": 0, "lon": 1, "lat": 1}},
        {"$limit": limit},
    ]

    results = list(db.top_events.aggregate(pipeline))
    return pd.DataFrame(results if results else [])


def get_tone_extremes(limit: int = 10) -> dict:
    """
    Return the ``limit`` most-positive and most-negative countries by average tone.

    Returns
    -------
    dict with keys ``most_positive`` and ``most_negative``, each a list of dicts.
    """
    db = get_mongodb_client()["gdelt"]

    pipeline = [
        {
            "$facet": {
                "most_positive": [
                    {"$sort": {"Average_Tone": -1}},
                    {"$limit": limit},
                    {"$project": {"_id": 0, "Country_Name": 1, "Average_Tone": 1}},
                ],
                "most_negative": [
                    {"$sort": {"Average_Tone": 1}},
                    {"$limit": limit},
                    {"$project": {"_id": 0, "Country_Name": 1, "Average_Tone": 1}},
                ],
            }
        }
    ]

    results = list(db.tone_by_country.aggregate(pipeline))
    return results[0] if results else {"most_positive": [], "most_negative": []}


def get_user_queried_events(
    country: str | None = None,
    actor1: str | None = None,
    actor2: str | None = None,
    date_min=None,
    date_max=None,
    goldstein_min: float = -10.0,
    goldstein_max: float = 10.0,
    tone_min: float = -100.0,
    tone_max: float = 100.0,
    mentions_min: int = 0,
    mentions_max: int | None = None,
    limit: int = 50,
) -> pd.DataFrame:
    """
    Query the ``events`` collection with the supplied filters.

    All text filters (country, actor1, actor2) are case-insensitive regex
    matches so partial names work.  Omit a filter by passing ``None``.

    Parameters
    ----------
    country       : Partial / full ActionGeo_FullName match.
    actor1        : Partial / full Actor1Name match.
    actor2        : Partial / full Actor2Name match.
    date_min      : Lower bound for event_date (datetime).
    date_max      : Upper bound for event_date (datetime).
    goldstein_min : Minimum Goldstein scale value (−10 … +10).
    goldstein_max : Maximum Goldstein scale value (−10 … +10).
    tone_min      : Minimum average tone.
    tone_max      : Maximum average tone.
    mentions_min  : Minimum number of mentions.
    mentions_max  : Maximum number of mentions (None = no upper bound).
    limit         : Maximum number of results to return.

    Returns
    -------
    pandas.DataFrame – empty if nothing matched.
    """
    # --- Build the $match document dynamically ----------------------------
    match: dict = {}

    if country:
        match["ActionGeo_FullName"] = {"$regex": country.strip(), "$options": "i"}
    if actor1:
        match["Actor1Name"] = {"$regex": actor1.strip(), "$options": "i"}
    if actor2:
        match["Actor2Name"] = {"$regex": actor2.strip(), "$options": "i"}

    # Date range
    date_filter: dict = {}
    if date_min is not None:
        date_filter["$gte"] = date_min
    if date_max is not None:
        date_filter["$lte"] = date_max
    if date_filter:
        match["event_date"] = date_filter

    # Numeric range filters
    match["goldstein_scale"] = {"$gte": goldstein_min, "$lte": goldstein_max}
    match["avg_tone"] = {"$gte": tone_min, "$lte": tone_max}

    mentions_filter: dict = {"$gte": mentions_min}
    if mentions_max is not None:
        mentions_filter["$lte"] = mentions_max
    match["num_mentions"] = mentions_filter

    # --- Aggregation pipeline ---------------------------------------------
    pipeline = [
        {"$match": match},
        {"$sort": {"num_articles": -1}},
        {"$limit": limit},
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
                "SOURCEURL": 1,
            }
        },
    ]

    db = get_mongodb_client()["gdelt"]
    results = list(db.events.aggregate(pipeline))
    return pd.DataFrame(results if results else [])


def get_country_event_spikes(only_spikes: bool = False, limit: int = 50) -> pd.DataFrame:
    """
    Retrieve pre-computed country event spike results from MongoDB.

    Parameters
    ----------
    only_spikes : bool
        When True only countries flagged as ``is_spike = True`` are returned.
    limit : int
        Maximum number of rows to return.

    Returns
    -------
    pandas.DataFrame – empty if the collection does not exist yet.
    """
    db = get_mongodb_client()["gdelt"]

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
                "baseline_days_count": 1,
                "avg_baseline_per_day": 1,
                "spike_score": 1,
                "is_spike": 1,
            }
        },
    ]

    results = list(db["country_event_spike"].aggregate(pipeline))
    return pd.DataFrame(results if results else [])
