import os
from pymongo import MongoClient

# MongoDB connection settings from environment variables
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

_client = None

def get_client():
    """Get or create MongoDB client (singleton pattern)."""
    global _client
    if _client is None:
        _client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000,
            maxPoolSize=10,
            retryWrites=True
        )
    return _client

def get_db():
    """Get the database instance."""
    return get_client()[MONGO_DB]
