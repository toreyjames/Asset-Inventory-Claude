"""Asset query tools for OT Asset Inventory MCP Server."""

import json
from typing import Any

import aiosqlite

from ..db.connection import get_db


async def list_assets(
    asset_type: str | None = None,
    process_area: str | None = None,
    site: str | None = None,
    criticality: str | None = None,
    owner: str | None = None,
    has_gaps: bool | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    List OT assets with optional filtering.

    Args:
        asset_type: Filter by asset type (PLC, HMI, Sensor, Actuator, RTU, Gateway, Switch, Server)
        process_area: Filter by process area name or ID
        site: Filter by site name or ID
        criticality: Filter by criticality level (critical, high, medium, low)
        owner: Filter by owner name
        has_gaps: If True, only return assets with compliance gaps
        limit: Maximum results to return (default 50, max 100)

    Returns:
        List of assets with key attributes
    """
    db = await get_db()

    query = """
        SELECT a.*, pa.name as process_area_name, s.name as site_name
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        LEFT JOIN sites s ON a.site_id = s.id
        WHERE 1=1
    """
    params: list[Any] = []

    if asset_type:
        query += " AND a.type = ?"
        params.append(asset_type)

    if process_area:
        query += " AND (a.process_area_id = ? OR pa.name LIKE ?)"
        params.extend([process_area, f"%{process_area}%"])

    if site:
        query += " AND (a.site_id = ? OR s.name LIKE ?)"
        params.extend([site, f"%{site}%"])

    if criticality:
        query += " AND a.criticality = ?"
        params.append(criticality)

    if owner:
        query += " AND a.owner LIKE ?"
        params.append(f"%{owner}%")

    if has_gaps:
        query += " AND (a.owner IS NULL OR NOT a.in_cmms OR NOT a.documented OR NOT a.security_policy_applied)"

    query += f" ORDER BY a.criticality DESC, a.name LIMIT {min(limit, 100)}"

    async with db.execute(query, params) as cursor:
        rows = await cursor.fetchall()
        return [_row_to_asset_dict(row) for row in rows]


async def get_asset(asset_id: str) -> dict[str, Any] | None:
    """
    Get detailed information about a specific asset.

    Args:
        asset_id: The unique identifier of the asset

    Returns:
        Complete asset details including relationships and compliance status,
        or None if not found
    """
    db = await get_db()

    # Get asset details
    async with db.execute(
        """
        SELECT a.*, pa.name as process_area_name, s.name as site_name
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        LEFT JOIN sites s ON a.site_id = s.id
        WHERE a.id = ?
        """,
        [asset_id],
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None

        asset = _row_to_asset_dict(row)

    # Get relationships where this asset is the source
    async with db.execute(
        """
        SELECT r.*, a.name as target_name, a.type as target_type
        FROM relationships r
        JOIN assets a ON r.target_asset_id = a.id
        WHERE r.source_asset_id = ?
        """,
        [asset_id],
    ) as cursor:
        rows = await cursor.fetchall()
        asset["outgoing_relationships"] = [
            {
                "id": row["id"],
                "target_id": row["target_asset_id"],
                "target_name": row["target_name"],
                "target_type": row["target_type"],
                "relationship_type": row["relationship_type"],
                "verified": bool(row["verified"]),
                "inferred": bool(row["inferred"]),
                "description": row["description"],
            }
            for row in rows
        ]

    # Get relationships where this asset is the target
    async with db.execute(
        """
        SELECT r.*, a.name as source_name, a.type as source_type
        FROM relationships r
        JOIN assets a ON r.source_asset_id = a.id
        WHERE r.target_asset_id = ?
        """,
        [asset_id],
    ) as cursor:
        rows = await cursor.fetchall()
        asset["incoming_relationships"] = [
            {
                "id": row["id"],
                "source_id": row["source_asset_id"],
                "source_name": row["source_name"],
                "source_type": row["source_type"],
                "relationship_type": row["relationship_type"],
                "verified": bool(row["verified"]),
                "inferred": bool(row["inferred"]),
                "description": row["description"],
            }
            for row in rows
        ]

    # Get any review flags for this asset
    async with db.execute(
        """
        SELECT * FROM review_flags
        WHERE asset_id = ? AND status = 'open'
        """,
        [asset_id],
    ) as cursor:
        rows = await cursor.fetchall()
        asset["open_flags"] = [
            {
                "id": row["id"],
                "flag_type": row["flag_type"],
                "description": row["description"],
                "severity": row["severity"],
            }
            for row in rows
        ]

    # Add compliance summary
    asset["compliance_summary"] = {
        "has_owner": asset["owner"] is not None,
        "in_cmms": asset["in_cmms"],
        "documented": asset["documented"],
        "security_policy_applied": asset["security_policy_applied"],
        "verified": asset["last_verified"] is not None,
        "gap_count": sum([
            asset["owner"] is None,
            not asset["in_cmms"],
            not asset["documented"],
            not asset["security_policy_applied"],
        ]),
    }

    return asset


async def search_assets(
    query: str,
    fields: list[str] | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Search assets by text query across multiple fields.

    Args:
        query: Search text (searches name, manufacturer, model, notes, function by default)
        fields: Specific fields to search (optional)
        limit: Maximum results to return (default 20)

    Returns:
        Matching assets ranked by relevance
    """
    db = await get_db()

    # Default fields to search
    if fields is None:
        fields = ["name", "manufacturer", "model", "notes", "function", "id"]

    # Build search conditions
    conditions = []
    params: list[Any] = []
    search_term = f"%{query}%"

    for field in fields:
        if field in ["name", "manufacturer", "model", "notes", "function", "id", "owner", "ip_address"]:
            conditions.append(f"a.{field} LIKE ?")
            params.append(search_term)

    if not conditions:
        return []

    sql = f"""
        SELECT a.*, pa.name as process_area_name, s.name as site_name
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        LEFT JOIN sites s ON a.site_id = s.id
        WHERE {" OR ".join(conditions)}
        ORDER BY
            CASE WHEN a.name LIKE ? THEN 0 ELSE 1 END,
            a.criticality DESC,
            a.name
        LIMIT {min(limit, 50)}
    """
    params.append(search_term)  # For ORDER BY

    async with db.execute(sql, params) as cursor:
        rows = await cursor.fetchall()
        return [_row_to_asset_dict(row) for row in rows]


async def get_asset_count_by_type() -> dict[str, int]:
    """
    Get count of assets grouped by type.

    Returns:
        Dictionary mapping asset type to count
    """
    db = await get_db()

    async with db.execute(
        "SELECT type, COUNT(*) as count FROM assets GROUP BY type ORDER BY count DESC"
    ) as cursor:
        rows = await cursor.fetchall()
        return {row["type"]: row["count"] for row in rows}


async def get_asset_count_by_criticality() -> dict[str, int]:
    """
    Get count of assets grouped by criticality.

    Returns:
        Dictionary mapping criticality level to count
    """
    db = await get_db()

    async with db.execute(
        """
        SELECT COALESCE(criticality, 'unassigned') as criticality, COUNT(*) as count
        FROM assets GROUP BY criticality ORDER BY
            CASE criticality
                WHEN 'critical' THEN 1
                WHEN 'high' THEN 2
                WHEN 'medium' THEN 3
                WHEN 'low' THEN 4
                ELSE 5
            END
        """
    ) as cursor:
        rows = await cursor.fetchall()
        return {row["criticality"]: row["count"] for row in rows}


def _row_to_asset_dict(row: aiosqlite.Row) -> dict[str, Any]:
    """Convert database row to asset dictionary."""
    data = dict(row)

    # Parse JSON fields
    for field in ["protocols", "tags"]:
        if data.get(field):
            try:
                data[field] = json.loads(data[field])
            except (json.JSONDecodeError, TypeError):
                data[field] = []
        else:
            data[field] = []

    # Convert boolean fields
    for field in ["in_cmms", "documented", "security_policy_applied"]:
        data[field] = bool(data.get(field, False))

    return data
