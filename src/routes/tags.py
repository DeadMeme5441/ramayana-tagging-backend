"""
Tag routes for the Ramayana Tagging Engine.
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi.responses import JSONResponse
import re

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


@router.get("/popular-topics")
async def get_popular_main_topics(
    limit: int = Query(10, ge=1, le=50, description="Number of topics to return")
):
    """
    Get the most popular main topics based on the number of unique tags they contain.

    This endpoint returns main topics sorted by how many different tags they categorize,
    not by their occurrence count in the text. This helps identify the most
    comprehensive categories in the tagging system.

    Parameters:
    - limit: Maximum number of topics to return

    Returns:
    - List of main topics with their tag counts and sample tags
    """
    try:
        db = await get_database()
        popular_topics = await db.get_popular_main_topics(limit=limit)

        return {"popular_topics": popular_topics, "count": len(popular_topics)}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching popular topics: {str(e)}"
        )


@router.get("/suggestions")
async def get_tag_suggestions(
    query: str = Query(..., description="Partial tag name to get suggestions for"),
    limit: int = Query(
        10, ge=1, le=50, description="Maximum number of suggestions to return"
    ),
):
    """
    Get tag name suggestions based on partial input for autocomplete.

    This endpoint provides quick, lightweight suggestions for the autocomplete
    feature, searching both tag names and subject information.

    Parameters:
    - query: Partial tag name or subject info to search for
    - limit: Maximum number of suggestions to return

    Returns:
    - List of matching tag names and metadata for autocomplete
    """
    try:
        if not query or len(query.strip()) < 2:
            return {"suggestions": []}

        db = await get_database()

        # Get tag suggestions from database
        suggestions = await db.get_tag_suggestions(query, limit)

        # Format suggestions for easy consumption by frontend
        formatted_suggestions = []
        for suggestion in suggestions:
            # Determine if match is in name or subject_info for highlighting
            match_type = "name"
            if not re.search(query, suggestion["name"], re.IGNORECASE):
                match_type = "subject_info"

            formatted_suggestions.append(
                {
                    "name": suggestion["name"],
                    "main_topics": suggestion.get("main_topics", []),
                    "subject_info": suggestion.get("subject_info", []),
                    "occurrence_count": suggestion.get("occurrence_count", 0),
                    "match_type": match_type,
                }
            )

        return {"suggestions": formatted_suggestions}

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching tag suggestions: {str(e)}"
        )
