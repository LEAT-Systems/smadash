from pymongo import AsyncMongoClient
from mongodb_config import mongo_settings

class MongoClient:
    def __init__(self, uri):
        self.uri = uri
        self.client = AsyncMongoClient(uri)

    async def connect_mongodb(self):
        uri = mongo_settings.connection_string

        try:
            await self.client.admin.command('ping')
            print("Connected to MongoDB!")
            return self.client

        except Exception as e:
            raise Exception("An error occurred: ", e)

# # Run the async function
# asyncio.run(connect_mongodb())
