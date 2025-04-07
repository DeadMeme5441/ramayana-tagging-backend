"""
MongoDB connection and operations for the Ramayana Tagging Engine.
"""

import logging
from typing import List, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import IndexModel, ASCENDING
from src.config import MONGO_URL, DB_NAME

logger = logging.getLogger(__name__)


class Database:
    """MongoDB database manager for the Ramayana Tagging Engine."""

    _instance = None
    _client = None
    _db = None

    @classmethod
    async def get_instance(cls):
        """Get or create the singleton database instance."""
        if cls._instance is None:
            cls._instance = cls()
            await cls._instance.initialize()
        return cls._instance

    async def initialize(self):
        """Initialize the MongoDB connection and create collections with indices."""
        logger.info(f"Connecting to MongoDB at {MONGO_URL}")
        self._client = AsyncIOMotorClient(MONGO_URL)
        self._db = self._client[DB_NAME]

        # Create collections if they don't exist
        collections = await self._db.list_collection_names()

        if "tags" not in collections:
            logger.info("Creating 'tags' collection")
            await self._db.create_collection("tags")

        if "adhyayas" not in collections:
            logger.info("Creating 'adhyayas' collection")
            await self._db.create_collection("adhyayas")

        if "khandas" not in collections:
            logger.info("Creating 'khandas' collection")
            await self._db.create_collection("khandas")

        if "statistics" not in collections:
            logger.info("Creating 'statistics' collection")
            await self._db.create_collection("statistics")

        # Create indices
        logger.info("Creating collection indices")
        await self._db.tags.create_index("name", unique=True)
        await self._db.tags.create_index("main_topics")

        await self._db.adhyayas.create_index(
            [("khanda_id", ASCENDING), ("adhyaya_id", ASCENDING)], unique=True
        )
        await self._db.adhyayas.create_index("khanda_id")

        logger.info("Database initialization completed")

    @property
    def db(self):
        """Get the database instance."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._db

    async def clear_collections(self):
        """Clear all collections in preparation for reindexing."""
        logger.info("Clearing all collections for reindexing")
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        await self._db.tags.delete_many({})
        await self._db.adhyayas.delete_many({})
        await self._db.khandas.delete_many({})

    async def insert_adhyaya(self, adhyaya_metadata: Dict[str, Any]):
        """Insert or update an adhyaya document."""
        khanda_id = adhyaya_metadata["khanda_id"]
        adhyaya_id = adhyaya_metadata["adhyaya_id"]
        doc_id = f"{khanda_id}_{adhyaya_id}"

        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        await self._db.adhyayas.insert_one({"_id": doc_id, **adhyaya_metadata})

    async def insert_khanda(
        self, khanda_id: int, khanda_name: str, adhyaya_ids: List[int]
    ):
        """Insert a khanda document."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        await self._db.khandas.insert_one(
            {"_id": khanda_id, "name": khanda_name, "adhyayas": adhyaya_ids}
        )

    async def upsert_tag(
        self,
        tag_name: str,
        main_topics: List[str],
        subject_info: List[str],
        occurrences: List[Dict[str, Any]],
    ):
        """Insert or update a tag document."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        if not occurrences:
            return

        await self._db.tags.update_one(
            {"name": tag_name},
            {
                "$set": {
                    "name": tag_name,
                    "main_topics": main_topics,
                    "subject_info": subject_info,
                },
                "$addToSet": {"occurrences": {"$each": occurrences}},
            },
            upsert=True,
        )

    async def insert_statistics(self, statistics: Dict[str, Any]):
        """Insert indexing statistics."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        await self._db.statistics.delete_many({})
        await self._db.statistics.insert_one(statistics)

    async def get_latest_statistics(self) -> Optional[Dict[str, Any]]:
        """Get the latest indexing statistics."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        return await self._db.statistics.find_one({}, sort=[("timestamp", -1)])


# Create a function to get the database instance
async def get_database():
    """Get the database instance."""
    return await Database.get_instance()
