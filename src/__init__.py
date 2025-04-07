"""
Ramayana Tagging Engine - a tool for processing and searching tagged Ramayana texts.
"""

import logging
from src.config import LOG_LEVEL

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)
logger.info("Initializing Ramayana Tagging Engine")
