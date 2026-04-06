
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
