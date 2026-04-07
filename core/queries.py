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

def get_user_queried_events(country, date_min, date_max, goldstein_min, goldstein_max, tone_min, tone_max, limit=10):
    db = get_mongodb_client()["gdelt"]
    pipeline = [
        {
            "$facet": {
                "matching_events": [
                    {
                        "$match": {
                            "ActionGeo_FullName": country,
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
                              } }
                ]
            }
        }
    ]
    results = list(db.events.aggregate(pipeline))[0]
    return results if results else {"matching_events": []}