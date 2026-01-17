"""Database connection management for OT Asset Inventory."""

from pathlib import Path
from typing import AsyncGenerator
from contextlib import asynccontextmanager

import aiosqlite


class DatabaseManager:
    """Manages async SQLite database connections."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._connection: aiosqlite.Connection | None = None

    async def connect(self) -> aiosqlite.Connection:
        """Establish database connection."""
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row

        # Enable foreign keys
        await self._connection.execute("PRAGMA foreign_keys = ON")

        return self._connection

    async def disconnect(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    @property
    def connection(self) -> aiosqlite.Connection:
        """Get current connection or raise error."""
        if self._connection is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._connection

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """Context manager for database connection."""
        if self._connection is None:
            await self.connect()
        yield self._connection


# Global database instance (set during server initialization)
_db_manager: DatabaseManager | None = None


def get_db_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    if _db_manager is None:
        raise RuntimeError("Database manager not initialized.")
    return _db_manager


def set_db_manager(manager: DatabaseManager) -> None:
    """Set the global database manager instance."""
    global _db_manager
    _db_manager = manager


async def get_db() -> aiosqlite.Connection:
    """Get the database connection."""
    return get_db_manager().connection
