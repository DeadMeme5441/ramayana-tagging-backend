"""
Routes package for the Ramayana Tagging Engine.
"""

from fastapi import APIRouter

# Create main router
api_router = APIRouter()

# Import and include sub-routers
from src.routes.admin import router as admin_router

api_router.include_router(admin_router)
