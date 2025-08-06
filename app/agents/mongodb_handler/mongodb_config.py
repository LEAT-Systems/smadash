import os
from dotenv import load_dotenv

load_dotenv()

class MongoSettings:
    def __init__(self, connection_string=None, database=None):
        self.connection_string= connection_string
        self.database_name = database

connection_url = os.getenv("MONGO_CONNECTION_STRING")
database_name = os.getenv("MONGO_DATABASE_NAME")

mongo_settings = MongoSettings(connection_string=connection_url, database=database_name)