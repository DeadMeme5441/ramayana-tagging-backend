"""
Tag model for the Ramayana Tagging Engine.
"""

from typing import List, Dict, Tuple, Optional


class Tag:
    """Represents a tag found in an adhyaya with its positions and metadata."""

    def __init__(self, name: str, start_position: Optional[int] = None):
        self.name = name
        self.start_positions = [start_position] if start_position is not None else []
        self.end_positions = []
        self.pairs = []

        # Tag content categorization
        self.main_topics = []
        self.subject_info = []

        # Process tag name to extract topics and subject info
        self._process_tag_content()

    def _process_tag_content(self):
        """Parse tag name to extract main topics and subject information."""
        if not self.name:
            return

        # Split tag content by semicolon
        parts = self.name.split(";")
        parts = [part.strip() for part in parts if part.strip()]

        # If there's only one item, it's a main topic regardless of spaces
        if len(parts) == 1:
            self.main_topics.append(parts[0])
            return

        # For multiple items, categorize by presence of spaces
        for part in parts:
            if " " not in part:
                self.main_topics.append(part)
            else:
                self.subject_info.append(part)

        # If we have multiple main topics but no subject info,
        # move the last main topic to subject info

        if len(self.main_topics) > 1 and not self.subject_info:
            self.subject_info.append(self.main_topics.pop())

    def add_start_position(self, position: int):
        """Add a start position for this tag."""
        self.start_positions.append(position)

    def add_end_position(self, position: int):
        """Add an end position for this tag."""
        self.end_positions.append(position)

    def create_pairs(self):
        """Match start and end positions to create valid tag pairs."""
        # Sort positions to ensure correct pairing
        start_sorted = sorted(self.start_positions)
        end_sorted = sorted(self.end_positions)

        # Only pair matching numbers of opening and closing tags
        pair_count = min(len(start_sorted), len(end_sorted))
        self.pairs = [(start_sorted[i], end_sorted[i]) for i in range(pair_count)]

    def to_dict(self) -> Dict:
        """Convert the tag to a dictionary representation."""
        return {
            "name": self.name,
            "start_positions": self.start_positions,
            "end_positions": self.end_positions,
            "pairs": self.pairs,
            "main_topics": self.main_topics,
            "subject_info": self.subject_info,
        }
