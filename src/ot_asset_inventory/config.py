"""Configuration management for OT Asset Inventory."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """Application configuration."""

    db_path: Path
    log_level: str
    seed_sample_data: bool

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        # Default database path
        default_db_path = Path(__file__).parent.parent.parent.parent / "data" / "inventory.db"

        return cls(
            db_path=Path(os.getenv("OT_INVENTORY_DB_PATH", str(default_db_path))),
            log_level=os.getenv("OT_INVENTORY_LOG_LEVEL", "INFO"),
            seed_sample_data=os.getenv("OT_INVENTORY_SEED_DATA", "true").lower() == "true",
        )


# Global config instance
_config: Config | None = None


def get_config() -> Config:
    """Get the configuration instance."""
    global _config
    if _config is None:
        _config = Config.from_env()
    return _config


def set_config(config: Config) -> None:
    """Set the configuration instance."""
    global _config
    _config = config
