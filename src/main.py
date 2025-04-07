"""
Main application entry point for the Ramayana Tagging Engine.
"""

import logging
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware

from src.routes import api_router
from src.database.mongodb import get_database

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Ramayana Tagging Engine",
    description="A tool for processing and searching tagged Ramayana texts",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development - restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(api_router)


@app.on_event("startup")
async def startup_db_client():
    """Initialize database connection on startup."""
    logger.info("Initializing database connection")
    await get_database()


@app.get("/", tags=["health"])
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "message": "Ramayana Tagging Engine is running"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
