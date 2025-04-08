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

    async def get_all_tags(
        self,
        main_topic: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
        min_occurrences: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch all tags with optional filtering."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Build the query based on filters
        query = {}
        if main_topic:
            query["main_topics"] = main_topic

        # Only include tags with minimum number of occurrences
        if min_occurrences > 0:
            # Using $size to check array length for occurrences
            query["$expr"] = {"$gte": [{"$size": "$occurrences"}, min_occurrences]}

        # Get the tags with pagination
        cursor = (
            self._db.tags.find(
                query,
                # Projection to optimize the response
                {
                    "name": 1,
                    "main_topics": 1,
                    "subject_info": 1,
                    "occurrences_count": {"$size": "$occurrences"},
                },
            )
            .sort("name", 1)
            .skip(skip)
            .limit(limit)
        )

        return await cursor.to_list(length=limit)

    async def get_tag_count(
        self, main_topic: Optional[str] = None, min_occurrences: int = 0
    ) -> int:
        """Get the total count of tags for pagination."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Build the query based on filters
        query = {}
        if main_topic:
            query["main_topics"] = main_topic

        # Only include tags with minimum number of occurrences
        if min_occurrences > 0:
            query["$expr"] = {"$gte": [{"$size": "$occurrences"}, min_occurrences]}

        return await self._db.tags.count_documents(query)

    async def get_all_main_topics(self) -> List[str]:
        """Get a list of all main topics for filtering."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Use aggregation to get unique main_topics
        pipeline = [
            {"$unwind": "$main_topics"},
            {"$group": {"_id": "$main_topics"}},
            {"$sort": {"_id": 1}},
        ]

        cursor = self._db.tags.aggregate(pipeline)
        result = await cursor.to_list(
            length=1000
        )  # Assuming we won't have more than 1000 topics
        return [doc["_id"] for doc in result]

    async def get_khandas_structure(self) -> List[Dict[str, Any]]:
        """Get the hierarchical structure of khandas and adhyayas."""
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Get all khandas first
        khandas_cursor = self._db.khandas.find().sort("_id", 1)
        khandas = await khandas_cursor.to_list(
            length=100
        )  # Assuming fewer than 100 khandas

        # For each khanda, get adhyaya metadata
        result = []
        for khanda in khandas:
            khanda_id = khanda["_id"]
            khanda_data = {
                "id": khanda_id,
                "name": khanda["name"],
                "adhyaya_count": len(khanda["adhyayas"]),
                "adhyayas": [],
            }

            # Query adhyayas for this khanda
            adhyayas_cursor = self._db.adhyayas.find(
                {"khanda_id": khanda_id},
                # Only retrieve essential metadata, not the full content
                {
                    "_id": 1,
                    "adhyaya_id": 1,
                    "title": 1,
                    "tag_count": {"$size": "$tags"},
                },
            ).sort("adhyaya_id", 1)

            adhyayas = await adhyayas_cursor.to_list(length=1000)
            khanda_data["adhyayas"] = [
                {
                    "id": adhyaya["adhyaya_id"],
                    "title": adhyaya.get("title", f"Adhyaya {adhyaya['adhyaya_id']}"),
                    "tag_count": adhyaya.get("tag_count", 0),
                }
                for adhyaya in adhyayas
            ]

            result.append(khanda_data)

        return result

    async def search_tags(
        self,
        query: str,
        khanda_id: Optional[int] = None,
        adhyaya_id: Optional[int] = None,
        main_topic: Optional[str] = None,
        limit: int = 20,
        skip: int = 0,
        context_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Search for tags matching the query with optional filters.

        Parameters:
        - query: The tag name or pattern to search for
        - khanda_id: Filter by khanda ID
        - adhyaya_id: Filter by adhyaya ID
        - main_topic: Filter by main topic category
        - limit: Maximum number of results to return
        - skip: Number of results to skip (for pagination)
        - context_size: Number of characters to include as context around the match

        Returns:
        - List of matching tags with context snippets
        """
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Build the tag query
        tag_query = {"$regex": query, "$options": "i"}  # Case-insensitive regex

        # Build the occurrence query based on filters
        occurrence_query = {}
        if khanda_id is not None:
            occurrence_query["khanda_id"] = khanda_id
        if adhyaya_id is not None and khanda_id is not None:
            occurrence_query["adhyaya_id"] = adhyaya_id

        # Build the main query
        main_query = {"name": tag_query}
        if main_topic:
            main_query["main_topics"] = main_topic

        # Add occurrence filter as a $match condition if we have any
        pipeline = [
            {"$match": main_query},
            {
                "$project": {
                    "name": 1,
                    "main_topics": 1,
                    "subject_info": 1,
                    "occurrences": {
                        "$filter": {
                            "input": "$occurrences",
                            "as": "occurrence",
                            "cond": {
                                "$and": [
                                    # This is where we apply the occurrence filters
                                    *(
                                        [{"$eq": ["$$occurrence.khanda_id", khanda_id]}]
                                        if khanda_id is not None
                                        else []
                                    ),
                                    *(
                                        [
                                            {
                                                "$eq": [
                                                    "$$occurrence.adhyaya_id",
                                                    adhyaya_id,
                                                ]
                                            }
                                        ]
                                        if adhyaya_id is not None
                                        and khanda_id is not None
                                        else []
                                    ),
                                ]
                            },
                        }
                    },
                }
            },
            {
                "$match": {"occurrences": {"$ne": []}}
            },  # Only include tags with matching occurrences
            # Sort by khanda_id and adhyaya_id instead of tag name
            {"$addFields": {"firstOccurrence": {"$arrayElemAt": ["$occurrences", 0]}}},
            {
                "$sort": {
                    "firstOccurrence.khanda_id": 1,
                    "firstOccurrence.adhyaya_id": 1,
                }
            },
            {"$skip": skip},
            {"$limit": limit},
        ]

        # Execute the query
        cursor = self._db.tags.aggregate(pipeline)
        results = await cursor.to_list(length=limit)

        # Enhance results with context
        enhanced_results = []
        for result in results:
            tag_with_context = await self._enrich_tag_with_context(
                result, context_size=context_size
            )
            if tag_with_context:
                enhanced_results.append(tag_with_context)

        return enhanced_results

    async def _enrich_tag_with_context(
        self, tag_result: Dict[str, Any], context_size: int = 100
    ) -> Optional[Dict[str, Any]]:
        """
        Enhance tag results with text context from the adhyayas.

        Parameters:
        - tag_result: The tag document from the database
        - context_size: Number of characters to include as context

        Returns:
        - Enhanced tag document with context snippets
        """
        # Group occurrences by khanda and adhyaya for efficient processing
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        occurrences_by_adhyaya = {}
        for occurrence in tag_result.get("occurrences", []):
            khanda_id = occurrence.get("khanda_id")
            adhyaya_id = occurrence.get("adhyaya_id")

            if khanda_id is None or adhyaya_id is None:
                continue

            key = f"{khanda_id}_{adhyaya_id}"
            if key not in occurrences_by_adhyaya:
                occurrences_by_adhyaya[key] = {
                    "khanda_id": khanda_id,
                    "adhyaya_id": adhyaya_id,
                    "positions": [],
                }

            occurrences_by_adhyaya[key]["positions"].append(
                {"start": occurrence.get("start"), "end": occurrence.get("end")}
            )

        # Get context for each occurrence
        context_snippets = []
        for adhyaya_key, data in occurrences_by_adhyaya.items():
            khanda_id = data["khanda_id"]
            adhyaya_id = data["adhyaya_id"]

            # Get the adhyaya document
            adhyaya_doc = await self._db.adhyayas.find_one(
                {"khanda_id": khanda_id, "adhyaya_id": adhyaya_id}
            )

            if not adhyaya_doc or "content" not in adhyaya_doc:
                continue

            content = adhyaya_doc["content"]

            # Get khanda name
            khanda_doc = await self._db.khandas.find_one({"_id": khanda_id})
            khanda_name = khanda_doc["name"] if khanda_doc else f"Khanda {khanda_id}"

            # For each position, extract context
            for position in data["positions"]:
                start_pos = position.get("start")
                end_pos = position.get("end")

                if (
                    start_pos is None
                    or end_pos is None
                    or start_pos >= len(content)
                    or end_pos > len(content)
                ):
                    continue

                # Extract the context
                context_start = max(0, start_pos - context_size)
                context_end = min(len(content), end_pos + context_size)

                before_text = content[context_start:start_pos]
                match_text = content[start_pos:end_pos]
                after_text = content[end_pos:context_end]

                context_snippets.append(
                    {
                        "khanda_id": khanda_id,
                        "khanda_name": khanda_name,
                        "adhyaya_id": adhyaya_id,
                        "adhyaya_title": adhyaya_doc.get(
                            "title", f"Adhyaya {adhyaya_id}"
                        ),
                        "before_text": before_text,
                        "match_text": match_text,
                        "after_text": after_text,
                        "position": {"start": start_pos, "end": end_pos},
                    }
                )

        # Return the enhanced result
        result = {
            "tag_name": tag_result["name"],
            "main_topics": tag_result.get("main_topics", []),
            "subject_info": tag_result.get("subject_info", []),
            "matches": context_snippets,
            "match_count": len(context_snippets),
        }

        return result

    async def count_search_results(
        self,
        query: str,
        khanda_id: Optional[int] = None,
        adhyaya_id: Optional[int] = None,
        main_topic: Optional[str] = None,
    ) -> int:
        """
        Count the number of search results for pagination.

        Parameters:
        - query: The tag name or pattern to search for
        - khanda_id: Filter by khanda ID
        - adhyaya_id: Filter by adhyaya ID
        - main_topic: Filter by main topic category

        Returns:
        - Total count of matching results
        """
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Build the tag query
        tag_query = {"$regex": query, "$options": "i"}  # Case-insensitive regex

        # Build the occurrence query based on filters
        occurrence_query = {}
        if khanda_id is not None:
            occurrence_query["khanda_id"] = khanda_id
        if adhyaya_id is not None and khanda_id is not None:
            occurrence_query["adhyaya_id"] = adhyaya_id

        # Build the main query
        main_query = {"name": tag_query}
        if main_topic:
            main_query["main_topics"] = main_topic

        # Add occurrence filter as a $match condition if we have any
        pipeline = [
            {"$match": main_query},
            {
                "$project": {
                    "name": 1,
                    "occurrences": {
                        "$filter": {
                            "input": "$occurrences",
                            "as": "occurrence",
                            "cond": {
                                "$and": [
                                    # This is where we apply the occurrence filters
                                    *(
                                        [
                                            {
                                                "$eq": [
                                                    "$$occurrence.khanda_id",
                                                    khanda_id,
                                                ]
                                            }
                                        ]
                                        if khanda_id is not None
                                        else []
                                    ),
                                    *(
                                        [
                                            {
                                                "$eq": [
                                                    "$$occurrence.adhyaya_id",
                                                    adhyaya_id,
                                                ]
                                            }
                                        ]
                                        if adhyaya_id is not None
                                        and khanda_id is not None
                                        else []
                                    ),
                                ]
                            },
                        }
                    },
                }
            },
            {
                "$match": {"occurrences": {"$ne": []}}
            },  # Only include tags with matching occurrences
            {"$count": "total"},
        ]

        # Execute the query
        cursor = self._db.tags.aggregate(pipeline)
        result = await cursor.to_list(length=1)

        return result[0]["total"] if result else 0

    async def get_adhyaya_content(
        self, khanda_id: int, adhyaya_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve the complete content of a specific adhyaya with tag information.

        Parameters:
        - khanda_id: The khanda ID
        - adhyaya_id: The adhyaya ID

        Returns:
        - Complete adhyaya document with content and tag information
        """
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Get the adhyaya document
        adhyaya_doc = await self._db.adhyayas.find_one(
            {"khanda_id": khanda_id, "adhyaya_id": adhyaya_id}
        )

        if not adhyaya_doc:
            return None

        # Get khanda information
        khanda_doc = await self._db.khandas.find_one({"_id": khanda_id})
        if khanda_doc:
            adhyaya_doc["khanda_name"] = khanda_doc.get("name", f"Khanda {khanda_id}")

        # Get navigation information
        adhyaya_doc["navigation"] = await self._get_adhyaya_navigation(
            khanda_id, adhyaya_id
        )

        # Structure tags for easier frontend use
        adhyaya_doc["structured_tags"] = await self._structure_adhyaya_tags(adhyaya_doc)

        return adhyaya_doc

    async def _get_adhyaya_navigation(
        self, khanda_id: int, adhyaya_id: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get previous and next adhyaya information for navigation.

        Parameters:
        - khanda_id: The khanda ID
        - adhyaya_id: The adhyaya ID

        Returns:
        - Navigation links for previous and next adhyayas
        """
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        navigation = {"previous": None, "next": None}

        # Get khanda information to determine boundaries
        khanda_doc = await self._db.khandas.find_one({"_id": khanda_id})
        if not khanda_doc:
            return navigation

        adhyaya_ids = sorted(khanda_doc.get("adhyayas", []))

        # Find the current adhyaya index
        if adhyaya_id not in adhyaya_ids:
            return navigation

        current_index = adhyaya_ids.index(adhyaya_id)

        # Get previous adhyaya
        if current_index > 0:
            prev_adhyaya_id = adhyaya_ids[current_index - 1]
            prev_adhyaya = await self._db.adhyayas.find_one(
                {"khanda_id": khanda_id, "adhyaya_id": prev_adhyaya_id},
                {"title": 1},
            )
            if prev_adhyaya:
                navigation["previous"] = {
                    "khanda_id": khanda_id,
                    "adhyaya_id": prev_adhyaya_id,
                    "title": prev_adhyaya.get("title", f"Adhyaya {prev_adhyaya_id}"),
                }
        # Check if we need to go to previous khanda
        elif khanda_id > 1:
            prev_khanda_doc = await self._db.khandas.find_one({"_id": khanda_id - 1})
            if prev_khanda_doc and prev_khanda_doc.get("adhyayas"):
                prev_adhyaya_ids = sorted(prev_khanda_doc.get("adhyayas", []))
                if prev_adhyaya_ids:
                    prev_adhyaya_id = prev_adhyaya_ids[
                        -1
                    ]  # Last adhyaya of previous khanda
                    prev_adhyaya = await self._db.adhyayas.find_one(
                        {"khanda_id": khanda_id - 1, "adhyaya_id": prev_adhyaya_id},
                        {"title": 1},
                    )
                    if prev_adhyaya:
                        navigation["previous"] = {
                            "khanda_id": khanda_id - 1,
                            "adhyaya_id": prev_adhyaya_id,
                            "title": prev_adhyaya.get(
                                "title", f"Adhyaya {prev_adhyaya_id}"
                            ),
                            "khanda_name": prev_khanda_doc.get(
                                "name", f"Khanda {khanda_id - 1}"
                            ),
                        }

        # Get next adhyaya
        if current_index < len(adhyaya_ids) - 1:
            next_adhyaya_id = adhyaya_ids[current_index + 1]
            next_adhyaya = await self._db.adhyayas.find_one(
                {"khanda_id": khanda_id, "adhyaya_id": next_adhyaya_id},
                {"title": 1},
            )
            if next_adhyaya:
                navigation["next"] = {
                    "khanda_id": khanda_id,
                    "adhyaya_id": next_adhyaya_id,
                    "title": next_adhyaya.get("title", f"Adhyaya {next_adhyaya_id}"),
                }
        # Check if we need to go to next khanda
        elif khanda_id < 7:  # Assuming 7 khandas total
            next_khanda_doc = await self._db.khandas.find_one({"_id": khanda_id + 1})
            if next_khanda_doc and next_khanda_doc.get("adhyayas"):
                next_adhyaya_ids = sorted(next_khanda_doc.get("adhyayas", []))
                if next_adhyaya_ids:
                    next_adhyaya_id = next_adhyaya_ids[
                        0
                    ]  # First adhyaya of next khanda
                    next_adhyaya = await self._db.adhyayas.find_one(
                        {"khanda_id": khanda_id + 1, "adhyaya_id": next_adhyaya_id},
                        {"title": 1},
                    )
                    if next_adhyaya:
                        navigation["next"] = {
                            "khanda_id": khanda_id + 1,
                            "adhyaya_id": next_adhyaya_id,
                            "title": next_adhyaya.get(
                                "title", f"Adhyaya {next_adhyaya_id}"
                            ),
                            "khanda_name": next_khanda_doc.get(
                                "name", f"Khanda {khanda_id + 1}"
                            ),
                        }

        return navigation

    async def _structure_adhyaya_tags(
        self, adhyaya_doc: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Structure tag information for easier frontend use.

        Parameters:
        - adhyaya_doc: The adhyaya document

        Returns:
        - Structured tag information
        """
        if not adhyaya_doc or "tags" not in adhyaya_doc:
            return {}

        # Group tags by category
        tags_by_category = {}
        position_map = {}  # Map positions to tag information

        for tag in adhyaya_doc.get("tags", []):
            # Skip tags with no pairs
            if not tag.get("pairs"):
                continue

            # Get the first main topic as category
            main_topics = tag.get("main_topics", [])

            if not main_topics:
                main_topics = ["Uncategorized"]

            for category in main_topics:
                # category = main_topics[0] if main_topics else "Uncategorized"

                # Add to category map
                if category not in tags_by_category:
                    tags_by_category[category] = []

                # Create simplified tag entry
                tag_entry = {
                    "name": tag.get("name", ""),
                    "main_topics": main_topics,
                    "subject_info": tag.get("subject_info", []),
                    "pairs": tag.get("pairs", []),
                }

                tags_by_category[category].append(tag_entry)

                # Add to position map for each pair
                for start, end in tag.get("pairs", []):
                    position_key = f"{start}_{end}"
                    position_map[position_key] = {
                        "tag_name": tag.get("name", ""),
                        "category": category,
                        "start": start,
                        "end": end,
                    }

        # Get flattened positions for highlighting
        highlight_positions = []
        for tag in adhyaya_doc.get("tags", []):
            for start, end in tag.get("pairs", []):
                highlight_positions.append(
                    {"tag_name": tag.get("name", ""), "start": start, "end": end}
                )

        # Sort by start position for efficient rendering
        highlight_positions.sort(key=lambda x: x["start"])

        return {
            "by_category": tags_by_category,
            "position_map": position_map,
            "highlight_positions": highlight_positions,
        }

    async def get_popular_main_topics(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most popular main topics based on the number of tags they contain.

        This method finds main topics that categorize the most unique tags,
        not based on occurrence count in the text but on how many different
        tags fall under each main topic.

        Parameters:
        - limit: Maximum number of main topics to return

        Returns:
        - List of main topics with their tag counts and subject info
        """
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        # Use aggregation to group tags by main topic and collect subject info
        pipeline = [
            # Unwind the main_topics array to work with individual topics
            {"$unwind": "$main_topics"},
            # Filter out excluded topics
            {"$match": {"main_topics": {"$nin": ["राक्षसः", "रावणः"]}}},
            # Group by main topic and collect data
            {
                "$group": {
                    "_id": "$main_topics",
                    "tag_count": {"$sum": 1},
                    "total_occurrences": {
                        "$sum": {"$size": {"$ifNull": ["$occurrences", []]}}
                    },
                    "all_subject_info": {
                        "$push": {
                            "$cond": [
                                {
                                    "$gt": [
                                        {"$size": {"$ifNull": ["$subject_info", []]}},
                                        0,
                                    ]
                                },
                                {"$ifNull": ["$subject_info", []]},
                                [],
                            ]
                        }
                    },
                }
            },
            # Sort by tag count in descending order
            {"$sort": {"tag_count": -1}},
            # Limit to the requested number
            {"$limit": limit},
            # Transform the all_subject_info array (flatten and limit)
            {
                "$project": {
                    "_id": 0,
                    "name": "$_id",
                    "tag_count": 1,
                    "total_occurrences": 1,
                    "subject_info": {
                        "$slice": [
                            {
                                "$reduce": {
                                    "input": "$all_subject_info",
                                    "initialValue": [],
                                    "in": {"$concatArrays": ["$$value", "$$this"]},
                                }
                            },
                            10,  # Limit to 10 subject info items
                        ]
                    },
                }
            },
        ]

        cursor = self._db.tags.aggregate(pipeline)
        result = await cursor.to_list(length=limit)

        return result

    async def get_tag_suggestions(
        self, query: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get tag name suggestions based on partial input for autocomplete.

        Parameters:
        - query: Partial tag name or subject info to search for
        - limit: Maximum number of suggestions to return

        Returns:
        - List of matching tag names and metadata for autocomplete
        """
        if self._db is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        if not query or len(query.strip()) < 2:
            return []

        # Search for tags that match the query in name OR subject_info
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {
                            "name": {"$regex": query, "$options": "i"}
                        },  # Match in tag name
                        {
                            "subject_info": {
                                "$elemMatch": {"$regex": query, "$options": "i"}
                            }
                        },  # Match in subject info
                    ]
                }
            },
            # Project only the fields we need
            {
                "$project": {
                    "name": 1,
                    "main_topics": 1,
                    "subject_info": 1,
                    "occurrence_count": {"$size": "$occurrences"},
                }
            },
            # Sort by most relevant (name matches first, then occurrence count)
            {
                "$addFields": {
                    "name_match": {
                        "$cond": [
                            {
                                "$regexMatch": {
                                    "input": "$name",
                                    "regex": "^" + query,
                                    "options": "i",
                                }
                            },
                            10,  # Prioritize matches at beginning of tag name
                            {
                                "$cond": [
                                    {
                                        "$regexMatch": {
                                            "input": "$name",
                                            "regex": query,
                                            "options": "i",
                                        }
                                    },
                                    5,  # Then matches anywhere in tag name
                                    0,  # Lower priority for subject info matches only
                                ]
                            },
                        ]
                    }
                }
            },
            {"$sort": {"name_match": -1, "occurrence_count": -1}},
            {"$limit": limit},
        ]

        cursor = self._db.tags.aggregate(pipeline)
        suggestions = await cursor.to_list(length=limit)

        return suggestions


# Create a function to get the database instance
async def get_database():
    """Get the database instance."""
    return await Database.get_instance()
