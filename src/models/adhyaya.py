"""
Adhyaya model for the Ramayana Tagging Engine.
"""

import os
import re
from typing import Dict, List, Optional, Any
import logging
from src.models.tag import Tag
from src.models.adhyaya_names import adhyaya_names

logger = logging.getLogger(__name__)


class AdhyayaTags:
    """Processes and extracts tag information from a single adhyaya file."""

    def __init__(self, file_path: str, khanda_id: int, adhyaya_id: int):
        self.file_path = file_path
        self.khanda_id = khanda_id
        self.adhyaya_id = adhyaya_id
        self.file_name = os.path.basename(file_path)

        # Read file content
        with open(file_path, "r", encoding="utf-8") as f:
            self.content = f.read()

        # Initialize tag collections
        self.tags = []  # List of Tag objects
        self.opening_errors = []  # Tags with opening but no closing
        self.closing_errors = []  # Tags with closing but no opening

        self.adhyaya_name = adhyaya_names.get(str(adhyaya_id), f"Sarga {adhyaya_id}")

        # Process the adhyaya
        self._find_tags()
        self._match_tags()
        self._identify_errors()

    def _find_tag_by_name(self, tag_name: str) -> Optional[Tag]:
        """Find a tag by name in the tags list."""
        for tag in self.tags:
            if tag.name == tag_name:
                return tag
        return None

    def _find_tags(self):
        """Find all opening and closing tags in the document."""
        # Find opening tags - matches anything like <tag> or <tag;info>
        opening_pattern = r"<([^/][^>]*)>"
        for match in re.finditer(opening_pattern, self.content):
            tag_name = match.group(1).strip()
            start_position = match.end()

            # Find existing tag or create new one
            tag = self._find_tag_by_name(tag_name)
            if tag is None:
                tag = Tag(tag_name, start_position)
                self.tags.append(tag)
            else:
                tag.add_start_position(start_position)

        # Find closing tags - matches anything like </tag> or </tag;info>
        closing_pattern = r"</([^>]*)>"
        for match in re.finditer(closing_pattern, self.content):
            tag_name = match.group(1).strip()
            end_position = match.start()

            # Find existing tag or create new one
            tag = self._find_tag_by_name(tag_name)
            if tag is None:
                tag = Tag(tag_name)
                self.tags.append(tag)

            tag.add_end_position(end_position)

    def _match_tags(self):
        """Match opening and closing tags to create pairs."""
        for tag in self.tags:
            tag.create_pairs()

    def _identify_errors(self):
        """Identify tags with errors (unmatched opening or closing)."""
        self.opening_errors = []
        self.closing_errors = []

        for tag in self.tags:
            if len(tag.start_positions) > len(tag.end_positions):
                self.opening_errors.append(tag.name)
            elif len(tag.start_positions) < len(tag.end_positions):
                self.closing_errors.append(tag.name)

    def _generate_organized_tags(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate a hierarchical organization of tags grouped by main topics."""
        organized = {}

        # Only process tags without errors
        valid_tags = [
            tag
            for tag in self.tags
            if tag.name not in self.opening_errors
            and tag.name not in self.closing_errors
        ]

        for tag in valid_tags:
            # Skip tags with no main topics
            if not tag.main_topics:
                continue

            # Get the first main topic as the primary category
            for main_topic in tag.main_topics:

                # Initialize the main topic if not present
                if main_topic not in organized:
                    organized[main_topic] = []

                # Create entry for this tag instance
                tag_entry = {
                    "full_tag": tag.name,
                    "subject_info": tag.subject_info,
                    "remaining_main_topics": (
                        tag.main_topics[1:] if len(tag.main_topics) > 1 else []
                    ),
                    "start_positions": tag.start_positions,
                    "end_positions": tag.end_positions,
                    "pairs": tag.pairs,
                }

                organized[main_topic].append(tag_entry)

        return organized

    def get_metadata(self) -> Dict[str, Any]:
        """Generate metadata for this adhyaya including all tag information."""
        return {
            "khanda_id": self.khanda_id,
            "adhyaya_id": self.adhyaya_id,
            "title": self.adhyaya_name,
            "file_path": self.file_path,
            "content": self.content,  # Include the content for full-text search
            "tags": [tag.to_dict() for tag in self.tags],
            "opening_errors": self.opening_errors,
            "closing_errors": self.closing_errors,
            "organized_tags": self._generate_organized_tags(),
        }

    def get_tag_errors(self) -> Dict[str, List[str]]:
        """Get lists of tags with errors."""
        return {
            "opening_errors": self.opening_errors,
            "closing_errors": self.closing_errors,
        }

    def get_valid_tag_count(self) -> int:
        """Get the number of valid tags (tags without errors)."""
        valid_count = 0
        for tag in self.tags:
            if (
                tag.name not in self.opening_errors
                and tag.name not in self.closing_errors
                and tag.pairs
            ):  # Only count tags with at least one valid pair
                valid_count += 1
        return valid_count
