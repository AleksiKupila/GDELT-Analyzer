from pymongo import MongoClient

class Mongo_client():

    def __init__(self, client_url: str = "mongodb://127.0.0.1:27017/"):
        self.client_url = client_url

        try:
            self.client = MongoClient(client_url)
            print(f"Succesfully connected to MongoDB at {client_url}")
        except Exception as e:
            print(f"Failed connecting to MongoDB: {e}")

    def new_db(self, name):

        return self.client[name]
    
    def drop_collections(self, db, collections):
        print("Dropping old collections...")
        for name in collections:
            db[name].drop()
        print("Old collections succesfully dropped!")

    def insert_data(self, db, collection, data):

        try:
            db[collection].insert_many(data)
            print("Data succesfully inserted into MongoDB!")
        except Exception as e:
            print(f"Failed to insert data into MongoDB: {e}")