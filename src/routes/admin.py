"""
Admin routes for the Ramayana Tagging Engine.
"""

import logging
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from fastapi.security.api_key import APIKeyHeader

from src.config import API_KEY
from src.database.mongodb import get_database
from src.services.indexer import RamayanaIndexer

logger = logging.getLogger(__name__)

# Security for admin-only endpoints
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_api_key(api_key: str = Depends(api_key_header)):
    """Validate the API key for admin endpoints."""
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Could not validate API key"
        )
    return api_key


# Create admin router
router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/reindex")
async def reindex_corpus(
    background_tasks: BackgroundTasks, api_key: str = Depends(get_api_key)
):
    """
    Re-index the entire Ramayana corpus (admin only endpoint)
    """
    # We'll run this in the background since it can take time
    background_tasks.add_task(reindex_corpus_task)

    return {
        "status": "indexing_started",
        "message": "Corpus re-indexing has been started in the background",
    }


async def reindex_corpus_task():
    """Background task to reindex the corpus"""
    try:
        indexer = RamayanaIndexer()
        stats = await indexer.build_indices()

        logger.info(f"Indexing completed: {stats}")
    except Exception as e:
        logger.error(f"Error during indexing: {str(e)}")
        # Log the full traceback
        import traceback

        logger.error(traceback.format_exc())


@router.get("/indexing-status")
async def get_indexing_status(api_key: str = Depends(get_api_key)):
    """
    Get the status of the last indexing operation (admin only endpoint)
    """
    db = await get_database()

    stats_doc = await db.get_latest_statistics()
    if not stats_doc:
        return {
            "status": "not_indexed",
            "message": "No indexing has been performed yet",
        }

    # Calculate total invalid tags
    opening_errors = stats_doc["invalid_tags"]["opening_errors"]
    closing_errors = stats_doc["invalid_tags"]["closing_errors"]
    total_invalid = len(opening_errors) + len(closing_errors)

    return {
        "status": "indexed",
        "statistics": stats_doc["count_stats"],
        "valid_tag_count": len(stats_doc["valid_tags"]),
        "invalid_tag_count": total_invalid,
        "opening_error_count": len(opening_errors),
        "closing_error_count": len(closing_errors),
    }


@router.get("/invalid-tags")
async def get_invalid_tags(
    limit: int = 100,
    skip: int = 0,
    error_type: str = "all",
    api_key: str = Depends(get_api_key),
):
    """
    Get detailed information about invalid tags for fixing (admin only endpoint)

    Parameters:
    - limit: Maximum number of errors to return
    - skip: Number of errors to skip (for pagination)
    - error_type: Filter by error type ('opening', 'closing', or 'all')
    """
    db = await get_database()

    stats_doc = await db.get_latest_statistics()
    if not stats_doc or "invalid_tags" not in stats_doc:
        return {
            "status": "no_errors_found",
            "message": "No indexing has been performed or no errors were found",
        }

    # Extract the errors based on the error_type parameter
    if error_type == "opening":
        errors = stats_doc["invalid_tags"]["opening_errors"][skip : skip + limit]
        total_count = len(stats_doc["invalid_tags"]["opening_errors"])
    elif error_type == "closing":
        errors = stats_doc["invalid_tags"]["closing_errors"][skip : skip + limit]
        total_count = len(stats_doc["invalid_tags"]["closing_errors"])
    else:  # 'all'
        opening_errors = stats_doc["invalid_tags"]["opening_errors"]
        closing_errors = stats_doc["invalid_tags"]["closing_errors"]
        all_errors = opening_errors + closing_errors
        errors = all_errors[skip : skip + limit]
        total_count = len(all_errors)

    return {
        "status": "success",
        "total_count": total_count,
        "returned_count": len(errors),
        "errors": errors,
        "pagination": {
            "limit": limit,
            "skip": skip,
            "has_more": (skip + limit) < total_count,
        },
    }
