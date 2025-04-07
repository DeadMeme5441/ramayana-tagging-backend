"""
Content retrieval routes for the Ramayana Tagging Engine.
"""

from typing import Dict, Any, Optional
from fastapi import APIRouter, Path, HTTPException, Depends
from pydantic import BaseModel

from src.database.mongodb import get_database

router = APIRouter(prefix="/api/content", tags=["content"])


class AdhyayaResponse(BaseModel):
    """Model for adhyaya content response."""

    khanda_id: int
    adhyaya_id: int
    khanda_name: str
    title: Optional[str] = None
    content: str
    navigation: Dict[str, Any]
    structured_tags: Dict[str, Any]


@router.get("/adhyaya/{khanda_id}/{adhyaya_id}", response_model=Dict[str, Any])
async def get_adhyaya_content(
    khanda_id: int = Path(..., description="The khanda ID", ge=1, le=7),
    adhyaya_id: int = Path(..., description="The adhyaya ID", ge=1),
):
    """
    Retrieve the complete content of a specific adhyaya with tag information.

    This endpoint returns the full text content of an adhyaya along with
    structured tag information for highlighting and navigation links to
    previous and next adhyayas.

    Parameters:
    - khanda_id: The khanda ID (1-7)
    - adhyaya_id: The adhyaya ID

    Returns:
    - Complete adhyaya data with content, tags, and navigation
    """
    try:
        db = await get_database()

        # Get the adhyaya content
        adhyaya = await db.get_adhyaya_content(
            khanda_id=khanda_id, adhyaya_id=adhyaya_id
        )

        if not adhyaya:
            raise HTTPException(
                status_code=404,
                detail=f"Adhyaya not found with khanda_id={khanda_id}, adhyaya_id={adhyaya_id}",
            )

        # Extract important fields for the response
        response = {
            "khanda_id": adhyaya["khanda_id"],
            "adhyaya_id": adhyaya["adhyaya_id"],
            "khanda_name": adhyaya.get("khanda_name", f"Khanda {khanda_id}"),
            "title": adhyaya.get("title", f"Adhyaya {adhyaya_id}"),
            "content": adhyaya["content"],
            "navigation": adhyaya["navigation"],
            "structured_tags": adhyaya["structured_tags"],
            "metadata": {
                "tag_count": len(adhyaya.get("tags", [])),
                "content_length": len(adhyaya["content"]),
            },
        }

        return response

    except Exception as e:
        # Log the exception here
        raise HTTPException(
            status_code=500, detail=f"Error retrieving adhyaya content: {str(e)}"
        )


@router.get("/tag/{khanda_id}/{adhyaya_id}/{tag_name}")
async def get_tag_in_adhyaya(
    khanda_id: int = Path(..., description="The khanda ID", ge=1, le=7),
    adhyaya_id: int = Path(..., description="The adhyaya ID", ge=1),
    tag_name: str = Path(..., description="The tag name"),
):
    """
    Get details about a specific tag within an adhyaya.

    This endpoint provides detailed information about a specific tag
    occurrence within an adhyaya, including its position and context.

    Parameters:
    - khanda_id: The khanda ID (1-7)
    - adhyaya_id: The adhyaya ID
    - tag_name: The tag name to find

    Returns:
    - Detailed tag information with context
    """
    try:
        db = await get_database()

        # Get the adhyaya content
        adhyaya = await db.get_adhyaya_content(
            khanda_id=khanda_id, adhyaya_id=adhyaya_id
        )

        if not adhyaya:
            raise HTTPException(
                status_code=404,
                detail=f"Adhyaya not found with khanda_id={khanda_id}, adhyaya_id={adhyaya_id}",
            )

        # Find the specific tag
        tag_data = None
        for tag in adhyaya.get("tags", []):
            if tag.get("name") == tag_name:
                tag_data = tag
                break

        if not tag_data:
            raise HTTPException(
                status_code=404, detail=f"Tag '{tag_name}' not found in this adhyaya"
            )

        # Extract context for each occurrence
        content = adhyaya["content"]
        occurrences = []

        for start, end in tag_data.get("pairs", []):
            # Get context before and after (100 characters each)
            context_start = max(0, start - 100)
            context_end = min(len(content), end + 100)

            occurrences.append(
                {
                    "start": start,
                    "end": end,
                    "before_text": content[context_start:start],
                    "match_text": content[start:end],
                    "after_text": content[end:context_end],
                    "position": {"start": start, "end": end},
                }
            )

        return {
            "tag_name": tag_name,
            "main_topics": tag_data.get("main_topics", []),
            "subject_info": tag_data.get("subject_info", []),
            "occurrences": occurrences,
            "occurrence_count": len(occurrences),
            "adhyaya_info": {
                "khanda_id": khanda_id,
                "adhyaya_id": adhyaya_id,
                "khanda_name": adhyaya.get("khanda_name", f"Khanda {khanda_id}"),
                "title": adhyaya.get("title", f"Adhyaya {adhyaya_id}"),
            },
        }

    except Exception as e:
        # Log the exception here
        raise HTTPException(
            status_code=500, detail=f"Error retrieving tag information: {str(e)}"
        )
