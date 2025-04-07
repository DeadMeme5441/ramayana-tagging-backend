"""
Indexing service for the Ramayana Tagging Engine.
"""

import os
import re
import logging
from typing import Dict, List, Set, Tuple, Any, Optional
from datetime import datetime

from src.models.adhyaya import AdhyayaTags
from src.database.mongodb import Database
from src.config import BASE_DIR

logger = logging.getLogger(__name__)


class RamayanaIndexer:
    """Creates and manages MongoDB indices for the entire Ramayana corpus."""

    def __init__(self, base_dir: str = BASE_DIR):
        self.base_dir = base_dir

        # Statistics for reporting
        self.stats = {
            "khanda_count": 0,
            "adhyaya_count": 0,
            "valid_tag_count": 0,
            "invalid_tag_count": 0,
            "opening_error_count": 0,
            "closing_error_count": 0,
            "start_time": None,
            "end_time": None,
            "duration": None,
        }

        # Valid vs invalid tag tracking
        self.valid_tags = set()
        # Detailed tracking of invalid tags with location information
        self.invalid_tags = {
            "opening_errors": [],  # Tags with opening but no closing
            "closing_errors": [],  # Tags with closing but no opening
        }

    async def build_indices(self):
        """Process all khandas and adhyayas to build MongoDB indices."""
        self.stats["start_time"] = datetime.now()

        # Get database instance
        db_instance = await Database.get_instance()

        # Clear existing data
        await db_instance.clear_collections()

        # Reset tracking variables
        self.valid_tags = set()
        self.invalid_tags = {
            "opening_errors": [],  # Tags with opening but no closing
            "closing_errors": [],  # Tags with closing but no opening
        }
        self.stats = {
            "khanda_count": 0,
            "adhyaya_count": 0,
            "valid_tag_count": 0,
            "invalid_tag_count": 0,
            "opening_error_count": 0,
            "closing_error_count": 0,
            "start_time": datetime.now(),
            "end_time": None,
            "duration": None,
        }

        # Get all khanda directories
        khanda_dirs = sorted(
            [
                d
                for d in os.listdir(self.base_dir)
                if os.path.isdir(os.path.join(self.base_dir, d))
            ]
        )

        logger.info(f"Found {len(khanda_dirs)} khanda directories")

        for khanda_dir in khanda_dirs:
            # Extract khanda ID from directory name
            khanda_match = re.match(r"(\d+)_.*", khanda_dir)
            if not khanda_match:
                logger.warning(f"Skipping directory with invalid format: {khanda_dir}")
                continue

            khanda_id = int(khanda_match.group(1))
            khanda_path = os.path.join(self.base_dir, khanda_dir)

            # Initialize khanda
            khanda_name = (
                khanda_dir.split("_", 1)[1] if "_" in khanda_dir else khanda_dir
            )
            logger.info(f"Processing khanda {khanda_id}: {khanda_name}")

            # Process each adhyaya in this khanda
            adhyaya_files = sorted(
                [
                    f
                    for f in os.listdir(khanda_path)
                    if f.endswith(".txt") and f[:-4].isdigit()
                ],
                key=lambda x: int(x[:-4]),
            )

            logger.info(
                f"Found {len(adhyaya_files)} adhyaya files in khanda {khanda_id}"
            )

            adhyaya_ids = []
            for adhyaya_file in adhyaya_files:
                adhyaya_id = int(adhyaya_file[:-4])
                adhyaya_path = os.path.join(khanda_path, adhyaya_file)

                logger.info(
                    f"Processing adhyaya {khanda_id}.{adhyaya_id}: {adhyaya_path}"
                )

                # Process this adhyaya
                await self._process_adhyaya(khanda_id, adhyaya_id, adhyaya_path)
                adhyaya_ids.append(adhyaya_id)

            # Add khanda to database
            await db_instance.insert_khanda(khanda_id, khanda_name, adhyaya_ids)

            self.stats["khanda_count"] += 1

        # Save statistics about valid and invalid tags
        self.stats["end_time"] = datetime.now()
        self.stats["duration"] = (
            self.stats["end_time"] - self.stats["start_time"]
        ).total_seconds()

        statistics_doc = {
            "valid_tags": list(self.valid_tags),
            "invalid_tags": self.invalid_tags,
            "count_stats": self.stats,
            "timestamp": datetime.now(),
        }

        await db_instance.insert_statistics(statistics_doc)

        logger.info(
            f"Indexing completed: processed {self.stats['khanda_count']} khandas, {self.stats['adhyaya_count']} adhyayas"
        )
        total_errors = len(self.invalid_tags["opening_errors"]) + len(
            self.invalid_tags["closing_errors"]
        )
        logger.info(
            f"Found {len(self.valid_tags)} valid tags and {total_errors} invalid tags"
        )
        logger.info(
            f"Opening tag errors: {len(self.invalid_tags['opening_errors'])}, Closing tag errors: {len(self.invalid_tags['closing_errors'])}"
        )
        logger.info(f"Duration: {self.stats['duration']} seconds")

        return self.stats

    async def _process_adhyaya(self, khanda_id: int, adhyaya_id: int, file_path: str):
        """Process a single adhyaya file to extract and index tags."""
        # Parse the adhyaya
        adhyaya_tags = AdhyayaTags(file_path, khanda_id, adhyaya_id)

        # Get metadata
        metadata = adhyaya_tags.get_metadata()

        # Get database instance
        db_instance = await Database.get_instance()

        # Add to adhyaya index - using composite key format
        await db_instance.insert_adhyaya(metadata)

        self.stats["adhyaya_count"] += 1

        tag_count = 0
        # Process tags for the tag index
        for tag in adhyaya_tags.tags:
            tag_name = tag.name

            # Check for tag errors
            has_error = False

            # Check if it's an opening error (has opening but no closing)
            if tag_name in adhyaya_tags.opening_errors:
                has_error = True
                # Record detailed information about the error
                for pos in tag.start_positions:
                    if len(tag.end_positions) < len(
                        tag.start_positions
                    ) and pos not in [p[0] for p in tag.pairs]:
                        # This position is one of the unmatched opening tags
                        self.invalid_tags["opening_errors"].append(
                            {
                                "tag_name": tag_name,
                                "khanda_id": khanda_id,
                                "adhyaya_id": adhyaya_id,
                                "position": pos,
                                "file_path": file_path,
                            }
                        )
                        self.stats["opening_error_count"] += 1

            # Check if it's a closing error (has closing but no opening)
            if tag_name in adhyaya_tags.closing_errors:
                has_error = True
                # Record detailed information about the error
                for pos in tag.end_positions:
                    if len(tag.start_positions) < len(
                        tag.end_positions
                    ) and pos not in [p[1] for p in tag.pairs]:
                        # This position is one of the unmatched closing tags
                        self.invalid_tags["closing_errors"].append(
                            {
                                "tag_name": tag_name,
                                "khanda_id": khanda_id,
                                "adhyaya_id": adhyaya_id,
                                "position": pos,
                                "file_path": file_path,
                            }
                        )
                        self.stats["closing_error_count"] += 1

            if has_error:
                continue

            # This is a valid tag
            self.valid_tags.add(tag_name)
            tag_count += 1

            # Prepare occurrences for this tag
            occurrences = []
            for start, end in tag.pairs:
                occurrences.append(
                    {
                        "khanda_id": khanda_id,
                        "adhyaya_id": adhyaya_id,
                        "start": start,
                        "end": end,
                    }
                )

            if not occurrences:
                continue

            # Add to tag collection with upsert for existing tags
            await db_instance.upsert_tag(
                tag_name=tag_name,
                main_topics=tag.main_topics,
                subject_info=tag.subject_info,
                occurrences=occurrences,
            )

        logger.info(
            f"Processed {tag_count} valid tags in adhyaya {khanda_id}.{adhyaya_id}"
        )
        self.stats["valid_tag_count"] += tag_count
