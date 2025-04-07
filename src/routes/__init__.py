"""
Routes package for the Ramayana Tagging Engine.
"""

from fastapi import APIRouter

# Create main router
api_router = APIRouter()

# Import and include sub-routers
from src.routes.admin import router as admin_router
from src.routes.tags import router as tags_router
from src.routes.navigation import router as navigation_router
from src.routes.search import router as search_router
from src.routes.content import router as content_router

api_router.include_router(admin_router)
api_router.include_router(tags_router)
api_router.include_router(navigation_router)
api_router.include_router(search_router)
api_router.include_router(content_router)
