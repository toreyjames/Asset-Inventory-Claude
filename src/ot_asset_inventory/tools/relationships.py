"""Relationship query tools for OT Asset Inventory MCP Server."""

from typing import Any

from ..db.connection import get_db
from ..utils.graph import (
    traverse_upstream,
    traverse_downstream,
    find_dependents,
    check_redundancy,
)


async def get_upstream(
    asset_id: str,
    relationship_types: list[str] | None = None,
    max_depth: int = 5,
) -> dict[str, Any]:
    """
    Get all assets upstream of the specified asset (assets that feed data to it).

    This follows relationships where other assets are the source and this asset
    is the target (e.g., sensors feeding data to a PLC).

    Args:
        asset_id: Starting asset ID
        relationship_types: Filter by specific types (e.g., ["feeds_data_to", "monitors"])
        max_depth: Maximum traversal depth (default 5)

    Returns:
        Hierarchical list of upstream assets with relationship details
    """
    result = await traverse_upstream(asset_id, relationship_types, max_depth)

    # Add summary
    result["summary"] = {
        "total_upstream_assets": len(result["assets"]),
        "assets_by_type": _count_by_key(result["assets"], "type"),
        "assets_by_criticality": _count_by_key(result["assets"], "criticality"),
    }

    return result


async def get_downstream(
    asset_id: str,
    relationship_types: list[str] | None = None,
    max_depth: int = 5,
) -> dict[str, Any]:
    """
    Get all assets downstream of the specified asset (assets that it feeds data to).

    This follows relationships where this asset is the source and other assets
    are the target (e.g., a PLC controlling actuators).

    Args:
        asset_id: Starting asset ID
        relationship_types: Filter by specific types (e.g., ["controls", "feeds_data_to"])
        max_depth: Maximum traversal depth (default 5)

    Returns:
        Hierarchical list of downstream assets with relationship details
    """
    result = await traverse_downstream(asset_id, relationship_types, max_depth)

    # Add summary
    result["summary"] = {
        "total_downstream_assets": len(result["assets"]),
        "assets_by_type": _count_by_key(result["assets"], "type"),
        "assets_by_criticality": _count_by_key(result["assets"], "criticality"),
    }

    return result


async def get_dependencies(asset_id: str, max_depth: int = 5) -> dict[str, Any]:
    """
    Get complete dependency map for an asset (both directions).

    This provides a comprehensive view of what feeds into this asset
    and what this asset feeds, along with explicit dependency relationships.

    Args:
        asset_id: Asset to analyze
        max_depth: Maximum traversal depth

    Returns:
        Complete dependency information including:
        - upstream: Assets that feed into this one
        - downstream: Assets this one feeds
        - depends_on: Assets this one explicitly depends on
        - dependents: Assets that explicitly depend on this one
        - redundancy: Any redundancy configuration
    """
    db = await get_db()

    # Get asset info
    async with db.execute(
        "SELECT id, name, type, criticality FROM assets WHERE id = ?",
        [asset_id],
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return {"error": f"Asset {asset_id} not found"}
        asset_info = {
            "id": row["id"],
            "name": row["name"],
            "type": row["type"],
            "criticality": row["criticality"],
        }

    upstream = await traverse_upstream(asset_id, max_depth=max_depth)
    downstream = await traverse_downstream(asset_id, max_depth=max_depth)
    dependents = await find_dependents(asset_id, max_depth=max_depth)
    redundancy = await check_redundancy(asset_id)

    # Get explicit depends_on relationships (what this asset depends on)
    async with db.execute(
        """
        SELECT a.id, a.name, a.type, a.criticality, r.description
        FROM relationships r
        JOIN assets a ON r.target_asset_id = a.id
        WHERE r.source_asset_id = ? AND r.relationship_type = 'depends_on'
        """,
        [asset_id],
    ) as cursor:
        rows = await cursor.fetchall()
        depends_on = [
            {
                "id": row["id"],
                "name": row["name"],
                "type": row["type"],
                "criticality": row["criticality"],
                "description": row["description"],
            }
            for row in rows
        ]

    return {
        "asset": asset_info,
        "upstream": {
            "count": len(upstream["assets"]),
            "assets": upstream["assets"],
        },
        "downstream": {
            "count": len(downstream["assets"]),
            "assets": downstream["assets"],
        },
        "depends_on": depends_on,
        "dependents": {
            "count": len(dependents["dependents"]),
            "assets": dependents["dependents"],
        },
        "redundancy": redundancy,
    }


async def list_relationships(
    source_asset_id: str | None = None,
    target_asset_id: str | None = None,
    relationship_type: str | None = None,
    verified_only: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    List relationships with optional filtering.

    Args:
        source_asset_id: Filter by source asset
        target_asset_id: Filter by target asset
        relationship_type: Filter by relationship type
        verified_only: Only return verified relationships
        limit: Maximum results

    Returns:
        List of relationships with asset details
    """
    db = await get_db()

    query = """
        SELECT r.*,
               s.name as source_name, s.type as source_type,
               t.name as target_name, t.type as target_type
        FROM relationships r
        JOIN assets s ON r.source_asset_id = s.id
        JOIN assets t ON r.target_asset_id = t.id
        WHERE 1=1
    """
    params: list[Any] = []

    if source_asset_id:
        query += " AND r.source_asset_id = ?"
        params.append(source_asset_id)

    if target_asset_id:
        query += " AND r.target_asset_id = ?"
        params.append(target_asset_id)

    if relationship_type:
        query += " AND r.relationship_type = ?"
        params.append(relationship_type)

    if verified_only:
        query += " AND r.verified = 1"

    query += f" ORDER BY r.relationship_type, s.name LIMIT {min(limit, 500)}"

    async with db.execute(query, params) as cursor:
        rows = await cursor.fetchall()
        return [
            {
                "id": row["id"],
                "source": {
                    "id": row["source_asset_id"],
                    "name": row["source_name"],
                    "type": row["source_type"],
                },
                "target": {
                    "id": row["target_asset_id"],
                    "name": row["target_name"],
                    "type": row["target_type"],
                },
                "relationship_type": row["relationship_type"],
                "inferred": bool(row["inferred"]),
                "verified": bool(row["verified"]),
                "description": row["description"],
            }
            for row in rows
        ]


async def get_relationship_types() -> list[dict[str, Any]]:
    """
    Get all relationship types with counts.

    Returns:
        List of relationship types with descriptions and usage counts
    """
    db = await get_db()

    type_descriptions = {
        "feeds_data_to": "Source sends data to target (sensor to PLC, PLC to HMI)",
        "controls": "Source controls target (PLC to actuator)",
        "monitors": "Source monitors target (HMI to PLC)",
        "safety_interlock_for": "Source is a safety interlock for target",
        "depends_on": "Source depends on target to function",
        "redundant_with": "Source and target provide redundancy for each other",
        "communicates_with": "Bidirectional communication between source and target",
        "powers": "Source provides power to target",
        "backs_up": "Source backs up target",
    }

    async with db.execute(
        """
        SELECT relationship_type, COUNT(*) as count,
               SUM(CASE WHEN verified THEN 1 ELSE 0 END) as verified_count,
               SUM(CASE WHEN inferred THEN 1 ELSE 0 END) as inferred_count
        FROM relationships
        GROUP BY relationship_type
        ORDER BY count DESC
        """
    ) as cursor:
        rows = await cursor.fetchall()
        return [
            {
                "type": row["relationship_type"],
                "description": type_descriptions.get(row["relationship_type"], ""),
                "count": row["count"],
                "verified_count": row["verified_count"],
                "inferred_count": row["inferred_count"],
            }
            for row in rows
        ]


def _count_by_key(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    """Count items by a specific key."""
    counts: dict[str, int] = {}
    for item in items:
        value = item.get(key) or "unassigned"
        counts[value] = counts.get(value, 0) + 1
    return counts
