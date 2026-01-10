import os
from pymongo import MongoClient

# MongoDB connection settings from environment variables
# Main database (for candidates, tasks, etc.)
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

# Teams database (separate MongoDB for teams data)
# If not set, fall back to using the same database as main
TEAMS_MONGO_URI = os.getenv("TEAMS_MONGO_URI") or MONGO_URI
TEAMS_MONGO_DB = os.getenv("TEAMS_MONGO_DB") or MONGO_DB

# Validate required environment variables
if not MONGO_URI:
    raise ValueError(
        "MONGO_URI environment variable is required. "
        "For local development: set it in your .env file. "
        "For Vercel: set it in Project Settings > Environment Variables."
    )
if not MONGO_DB:
    raise ValueError(
        "MONGO_DB environment variable is required. "
        "For local development: set it in your .env file. "
        "For Vercel: set it in Project Settings > Environment Variables."
    )

_client = None
_teams_client = None

def get_client():
    """Get or create main MongoDB client (singleton pattern)."""
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

def get_teams_client():
    """Get or create teams MongoDB client (singleton pattern)."""
    global _teams_client
    if _teams_client is None:
        _teams_client = MongoClient(
            TEAMS_MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000,
            maxPoolSize=10,
            retryWrites=True
        )
    return _teams_client

def get_db():
    """Get the main database instance."""
    return get_client()[MONGO_DB]

def get_teams_db():
    """Get the teams database instance."""
    return get_teams_client()[TEAMS_MONGO_DB]
