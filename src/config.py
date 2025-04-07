"""
Configuration settings for the Ramayana Tagging Engine.
"""

import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
MONGO_URL = os.environ.get("MONGO_URL", "")
DB_NAME = os.environ.get("DB_NAME", "ramayana")

# API Security
API_KEY = os.environ.get(
    "RAMAYANA_API_KEY", ""
)  # In production, this should be securely stored

# Data Directory
BASE_DIR = os.environ.get(
    "RAMAYANA_DATA_DIR", "ramayana"
)  # Path to Ramayana data directory

# Logging Configuration
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
