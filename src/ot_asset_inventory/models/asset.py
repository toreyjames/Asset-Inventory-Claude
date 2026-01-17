"""Asset data model for OT Asset Inventory."""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
import json


@dataclass
class Asset:
    """Represents an OT asset in the inventory."""

    id: str
    name: str
    type: str  # PLC, HMI, Sensor, Actuator, RTU, Gateway, Switch, Server, Workstation

    # Hardware details
    manufacturer: str | None = None
    model: str | None = None
    serial_number: str | None = None
    firmware_version: str | None = None

    # Location
    site_id: str | None = None
    building: str | None = None
    area: str | None = None
    zone: str | None = None
    process_area_id: str | None = None

    # Network
    ip_address: str | None = None
    mac_address: str | None = None
    vlan: int | None = None
    protocols: list[str] = field(default_factory=list)

    # Environment context
    environment_type: str | None = None
    function: str | None = None

    # Ownership
    owner: str | None = None
    maintainer: str | None = None
    last_verified: date | None = None

    # Compliance
    in_cmms: bool = False
    documented: bool = False
    security_policy_applied: bool = False

    # Risk
    criticality: str | None = None  # critical, high, medium, low

    # Metadata
    notes: str | None = None
    tags: list[str] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Any) -> "Asset":
        """Create Asset from database row."""
        data = dict(row)

        # Parse JSON fields
        if data.get("protocols"):
            try:
                data["protocols"] = json.loads(data["protocols"])
            except (json.JSONDecodeError, TypeError):
                data["protocols"] = []
        else:
            data["protocols"] = []

        if data.get("tags"):
            try:
                data["tags"] = json.loads(data["tags"])
            except (json.JSONDecodeError, TypeError):
                data["tags"] = []
        else:
            data["tags"] = []

        # Convert boolean fields
        data["in_cmms"] = bool(data.get("in_cmms", False))
        data["documented"] = bool(data.get("documented", False))
        data["security_policy_applied"] = bool(data.get("security_policy_applied", False))

        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_dict(self) -> dict[str, Any]:
        """Convert Asset to dictionary."""
        result = {}
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, (date, datetime)):
                value = value.isoformat()
            result[field_name] = value
        return result

    def to_summary(self) -> dict[str, Any]:
        """Return a summary view of the asset."""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "manufacturer": self.manufacturer,
            "model": self.model,
            "criticality": self.criticality,
            "process_area_id": self.process_area_id,
            "ip_address": self.ip_address,
            "owner": self.owner,
        }
