"""
Navigation routes for the Ramayana Tagging Engine.
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends

from src.database.mongodb import get_database

router = APIRouter(prefix="/api/navigation", tags=["navigation"])


@router.get("/khandas")
async def get_khandas():
    """
    Get the hierarchical structure of khandas and adhyayas.

    Returns:
    - List of khandas with their adhyayas
    """
    try:
        db = await get_database()
        khandas = await db.get_khandas_structure()

        return {"khandas": khandas, "count": len(khandas)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching khandas: {str(e)}")
