from pymongo import MongoClient

def get_mongodb_client():

    return MongoClient("mongodb://localhost:27017/")

def write_data(df, collection):
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

def drop_collections(db):

    print("Dropping old collections...")

    db["events"].drop()
    db["separate_events"].drop()
    db["top_negative_events"].drop()
    db["top_impact_events"].drop()
    db["top_events"].drop()
    db["events_per_country"].drop()
    db["tone_by_country"].drop()

    print("Old collections succesfully dropped!")