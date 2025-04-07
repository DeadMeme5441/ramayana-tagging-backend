"""
Search routes for the Ramayana Tagging Engine.
"""

from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel

from src.database.mongodb import get_database

router = APIRouter(prefix="/api/search", tags=["search"])


class SearchResult(BaseModel):
    """Model for search results."""

    tag_name: str
    main_topics: List[str]
    subject_info: List[str]
    matches: List[Dict[str, Any]]
    match_count: int


@router.get("/", response_model=Dict[str, Any])
async def search_tags(
    query: str = Query(..., description="Tag name or pattern to search for"),
    khanda_id: Optional[int] = Query(None, description="Filter by khanda ID"),
    adhyaya_id: Optional[int] = Query(None, description="Filter by adhyaya ID"),
    main_topic: Optional[str] = Query(
        None, description="Filter by main topic category"
    ),
    context_size: int = Query(
        100, ge=10, le=500, description="Number of characters to include as context"
    ),
    limit: int = Query(
        20, ge=1, le=100, description="Maximum number of results to return"
    ),
    skip: int = Query(0, ge=0, description="Number of results to skip for pagination"),
):
    """
    Search for tags in the Ramayana corpus.

    This endpoint allows searching for specific tags with optional filtering by
    khanda, adhyaya, or main topic category. The results include context snippets
    showing the text around each match.

    Parameters:
    - query: The tag name or pattern to search for
    - khanda_id: Filter by khanda ID
    - adhyaya_id: Filter by adhyaya ID
    - main_topic: Filter by main topic category
    - context_size: Number of characters to include as context
    - limit: Maximum number of results to return
    - skip: Number of results to skip (for pagination)

    Returns:
    - List of matching tags with context snippets
    - Pagination metadata
    - Aggregated statistics
    """
    try:
        # Validate adhyaya_id is only used with khanda_id
        if adhyaya_id is not None and khanda_id is None:
            raise HTTPException(
                status_code=400,
                detail="adhyaya_id can only be used together with khanda_id",
            )

        db = await get_database()

        # Get search results
        results = await db.search_tags(
            query=query,
            khanda_id=khanda_id,
            adhyaya_id=adhyaya_id,
            main_topic=main_topic,
            context_size=context_size,
            limit=limit,
            skip=skip,
        )

        # Get total count for pagination
        total_count = await db.count_search_results(
            query=query,
            khanda_id=khanda_id,
            adhyaya_id=adhyaya_id,
            main_topic=main_topic,
        )

        # Organize results by main topic for better frontend display
        results_by_category = {}
        for result in results:
            # Use the first main topic as the category
            main_topics = result.get("main_topics", [])
            category = main_topics[0] if main_topics else "Uncategorized"

            if category not in results_by_category:
                results_by_category[category] = []

            results_by_category[category].append(result)

        # Calculate total matches across all results
        total_matches = sum(result.get("match_count", 0) for result in results)

        return {
            "results": results,
            "results_by_category": results_by_category,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "skip": skip,
                "has_more": (skip + limit) < total_count,
            },
            "statistics": {
                "tag_count": len(results),
                "match_count": total_matches,
                "categories": list(results_by_category.keys()),
            },
            "filters": {
                "query": query,
                "khanda_id": khanda_id,
                "adhyaya_id": adhyaya_id,
                "main_topic": main_topic,
            },
        }

    except Exception as e:
        # Log the exception here
        raise HTTPException(status_code=500, detail=f"Error searching tags: {str(e)}")
