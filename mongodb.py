from pymongo import MongoClient

class Mongo_client():

    def __init__(self, client_url: str = "mongodb://127.0.0.1:27017/"):
        self.client_url = client_url
        self.collections = []

        try:
            self.client = MongoClient(client_url)
            print(f"Succesfully connected to MongoDB at {client_url}")
        except Exception as e:
            print(f"Failed connecting to MongoDB: {e}")

    def new_db(self, name):
        self.collections.append("name")
        return self.client[name]
    
    def drop_collections(self, collections):
        for name in collections:
            self.client.drop_database(name)