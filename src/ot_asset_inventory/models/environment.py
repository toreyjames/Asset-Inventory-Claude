"""Environment, Site, and ProcessArea data models for OT Asset Inventory."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Environment:
    """Represents a top-level environment (e.g., facility type)."""

    id: str
    name: str
    type: str  # manufacturing, water_treatment, energy, chemical, food_beverage, pharmaceutical
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Environment":
        """Create Environment from database row."""
        data = dict(row)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, datetime):
                value = value.isoformat()
            result[field_name] = value
        return result


@dataclass
class Site:
    """Represents a physical site within an environment."""

    id: str
    environment_id: str
    name: str
    address: str | None = None
    timezone: str | None = None
    created_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Site":
        """Create Site from database row."""
        data = dict(row)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, datetime):
                value = value.isoformat()
            result[field_name] = value
        return result


@dataclass
class ProcessArea:
    """Represents a process area within a site."""

    id: str
    site_id: str
    name: str
    description: str | None = None
    function: str | None = None
    created_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Any) -> "ProcessArea":
        """Create ProcessArea from database row."""
        data = dict(row)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, datetime):
                value = value.isoformat()
            result[field_name] = value
        return result


# Valid environment types
ENVIRONMENT_TYPES = [
    "manufacturing",
    "water_treatment",
    "energy",
    "chemical",
    "food_beverage",
    "pharmaceutical",
]
