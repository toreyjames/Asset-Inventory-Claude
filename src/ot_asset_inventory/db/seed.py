"""Sample data seeding for OT Asset Inventory."""

import json
from pathlib import Path
from typing import Any
import uuid

import aiosqlite


async def seed_sample_data(db: aiosqlite.Connection, data_path: Path | None = None) -> None:
    """Load sample data into the database if empty."""
    # Check if data already exists
    async with db.execute("SELECT COUNT(*) FROM assets") as cursor:
        row = await cursor.fetchone()
        if row and row[0] > 0:
            return  # Data already exists

    # Default data path
    if data_path is None:
        data_path = Path(__file__).parent.parent.parent.parent / "data" / "sample_data.json"

    if not data_path.exists():
        return

    with open(data_path) as f:
        data = json.load(f)

    await _seed_environments(db, data.get("environments", []))
    await _seed_sites(db, data.get("sites", []))
    await _seed_process_areas(db, data.get("process_areas", []))
    await _seed_assets(db, data.get("assets", []))
    await _seed_relationships(db, data.get("relationships", []))

    await db.commit()


async def _seed_environments(db: aiosqlite.Connection, environments: list[dict[str, Any]]) -> None:
    """Seed environment data."""
    for env in environments:
        await db.execute(
            """
            INSERT OR IGNORE INTO environments (id, name, type, description)
            VALUES (?, ?, ?, ?)
            """,
            (env["id"], env["name"], env["type"], env.get("description")),
        )


async def _seed_sites(db: aiosqlite.Connection, sites: list[dict[str, Any]]) -> None:
    """Seed site data."""
    for site in sites:
        await db.execute(
            """
            INSERT OR IGNORE INTO sites (id, environment_id, name, address, timezone)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                site["id"],
                site["environment_id"],
                site["name"],
                site.get("address"),
                site.get("timezone"),
            ),
        )


async def _seed_process_areas(db: aiosqlite.Connection, process_areas: list[dict[str, Any]]) -> None:
    """Seed process area data."""
    for pa in process_areas:
        await db.execute(
            """
            INSERT OR IGNORE INTO process_areas (id, site_id, name, description, function)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                pa["id"],
                pa["site_id"],
                pa["name"],
                pa.get("description"),
                pa.get("function"),
            ),
        )


async def _seed_assets(db: aiosqlite.Connection, assets: list[dict[str, Any]]) -> None:
    """Seed asset data."""
    for asset in assets:
        # Convert lists to JSON strings
        protocols = json.dumps(asset.get("protocols", []))
        tags = json.dumps(asset.get("tags", []))

        await db.execute(
            """
            INSERT OR IGNORE INTO assets (
                id, name, type, manufacturer, model, serial_number, firmware_version,
                site_id, building, area, zone, process_area_id,
                ip_address, mac_address, vlan, protocols,
                environment_type, function,
                owner, maintainer, last_verified,
                in_cmms, documented, security_policy_applied,
                criticality, notes, tags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset["id"],
                asset["name"],
                asset["type"],
                asset.get("manufacturer"),
                asset.get("model"),
                asset.get("serial_number"),
                asset.get("firmware_version"),
                asset.get("site_id"),
                asset.get("building"),
                asset.get("area"),
                asset.get("zone"),
                asset.get("process_area_id"),
                asset.get("ip_address"),
                asset.get("mac_address"),
                asset.get("vlan"),
                protocols,
                asset.get("environment_type"),
                asset.get("function"),
                asset.get("owner"),
                asset.get("maintainer"),
                asset.get("last_verified"),
                1 if asset.get("in_cmms") else 0,
                1 if asset.get("documented") else 0,
                1 if asset.get("security_policy_applied") else 0,
                asset.get("criticality"),
                asset.get("notes"),
                tags,
            ),
        )


async def _seed_relationships(db: aiosqlite.Connection, relationships: list[dict[str, Any]]) -> None:
    """Seed relationship data."""
    for rel in relationships:
        rel_id = rel.get("id") or str(uuid.uuid4())
        await db.execute(
            """
            INSERT OR IGNORE INTO relationships (
                id, source_asset_id, target_asset_id, relationship_type,
                inferred, verified, description
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rel_id,
                rel["source_asset_id"],
                rel["target_asset_id"],
                rel["relationship_type"],
                1 if rel.get("inferred") else 0,
                1 if rel.get("verified") else 0,
                rel.get("description"),
            ),
        )


async def clear_all_data(db: aiosqlite.Connection) -> None:
    """Clear all data from the database (for testing)."""
    tables = ["audit_log", "review_flags", "relationships", "assets",
              "compliance_frameworks", "process_areas", "sites", "environments"]
    for table in tables:
        await db.execute(f"DELETE FROM {table}")
    await db.commit()
