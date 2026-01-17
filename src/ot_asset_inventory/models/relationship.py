"""Relationship data model for OT Asset Inventory."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Relationship:
    """Represents a relationship between two assets."""

    id: str
    source_asset_id: str
    target_asset_id: str
    relationship_type: str  # feeds_data_to, controls, monitors, safety_interlock_for, depends_on, redundant_with

    inferred: bool = False
    verified: bool = False
    verified_by: str | None = None
    verified_at: datetime | None = None
    description: str | None = None
    created_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Relationship":
        """Create Relationship from database row."""
        data = dict(row)

        # Convert boolean fields
        data["inferred"] = bool(data.get("inferred", False))
        data["verified"] = bool(data.get("verified", False))

        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_dict(self) -> dict[str, Any]:
        """Convert Relationship to dictionary."""
        result = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, datetime):
                value = value.isoformat()
            result[field_name] = value
        return result


# Valid relationship types
RELATIONSHIP_TYPES = [
    "feeds_data_to",      # Source sends data to target
    "controls",           # Source controls target
    "monitors",           # Source monitors target
    "safety_interlock_for",  # Source is a safety interlock for target
    "depends_on",         # Source depends on target
    "redundant_with",     # Source is redundant with target
    "communicates_with",  # Bidirectional communication
    "powers",             # Source provides power to target
    "backs_up",           # Source backs up target
]
