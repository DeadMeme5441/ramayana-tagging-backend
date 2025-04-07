"""
Tag routes for the Ramayana Tagging Engine.
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi.responses import JSONResponse

from src.database.mongodb import get_database

router = APIRouter(prefix="/api/tags", tags=["tags"])


@router.get("/")
async def get_tags(
    main_topic: Optional[str] = None,
    min_occurrences: int = Query(0, ge=0, description="Minimum number of occurrences"),
    limit: int = Query(100, ge=1, le=500, description="Number of tags to return"),
    skip: int = Query(0, ge=0, description="Number of tags to skip for pagination"),
):
    """
    Get a list of tags with optional filtering.

    Parameters:
    - main_topic: Filter by main topic
    - min_occurrences: Only return tags with at least this many occurrences
    - limit: Maximum number of tags to return (for pagination)
    - skip: Number of tags to skip (for pagination)

    Returns:
    - List of tags with their metadata
    - Pagination information
    """
    try:
        db = await get_database()

        # Get the tags based on filters
        tags = await db.get_all_tags(
            main_topic=main_topic,
            min_occurrences=min_occurrences,
            limit=limit,
            skip=skip,
        )

        # Get the total count for pagination
        total_count = await db.get_tag_count(
            main_topic=main_topic, min_occurrences=min_occurrences
        )

        return {
            "tags": tags,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "skip": skip,
                "has_more": (skip + limit) < total_count,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching tags: {str(e)}")


@router.get("/main-topics")
async def get_main_topics():
    """
    Get a list of all main topics for filtering.

    Returns:
    - List of distinct main topics used in tags
    """
    try:
        db = await get_database()
        main_topics = await db.get_all_main_topics()

        return {"main_topics": main_topics, "count": len(main_topics)}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching main topics: {str(e)}"
        )
