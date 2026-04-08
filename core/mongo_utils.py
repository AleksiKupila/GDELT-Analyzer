from pymongo import MongoClient

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

def drop_collections(db, all_collections = True):
    '''
    Drops GDELT collections in DB
    If all_collections = False, only drops analysis collections
    '''
    if all_collections:
        print("Dropping all old collections...")
        db["events"].drop()

    else:
        print("Dropping old analysis collections...")
        
    db["separate_events"].drop()
    db["top_negative_events"].drop()
    db["top_impact_events"].drop()
    db["top_events"].drop()
    db["events_per_country"].drop()
    db["tone_by_country"].drop()
    db["country_event_spike"].drop()

    print("Old collections succesfully dropped!")
