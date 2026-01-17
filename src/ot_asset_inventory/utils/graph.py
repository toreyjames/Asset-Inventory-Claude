"""Graph traversal utilities for asset relationships."""

from collections import deque
from typing import Any

import aiosqlite

from ..db.connection import get_db


async def traverse_upstream(
    asset_id: str,
    relationship_types: list[str] | None = None,
    max_depth: int = 5,
) -> dict[str, Any]:
    """
    BFS traversal to find all upstream assets (assets that feed into this one).

    Args:
        asset_id: Starting asset ID
        relationship_types: Filter by specific relationship types
        max_depth: Maximum traversal depth

    Returns:
        Dictionary containing root asset ID, upstream assets with depth info
    """
    db = await get_db()

    visited: set[str] = set()
    result: dict[str, Any] = {
        "root": asset_id,
        "assets": [],
        "depth_map": {},
        "max_depth_reached": max_depth,
    }
    queue: deque[tuple[str, int]] = deque([(asset_id, 0)])

    while queue:
        current_id, depth = queue.popleft()

        if current_id in visited or depth > max_depth:
            continue
        visited.add(current_id)

        # Skip root asset in results but continue traversal
        if current_id == asset_id:
            pass  # Don't add root to results
        else:
            # Get asset details
            async with db.execute(
                "SELECT id, name, type, criticality, process_area_id FROM assets WHERE id = ?",
                [current_id],
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    result["assets"].append({
                        "id": row["id"],
                        "name": row["name"],
                        "type": row["type"],
                        "criticality": row["criticality"],
                        "process_area_id": row["process_area_id"],
                        "depth": depth,
                    })
                    result["depth_map"][current_id] = depth

        # Find assets that feed into the current one (source -> current)
        query = """
            SELECT r.source_asset_id, r.relationship_type, r.description
            FROM relationships r
            WHERE r.target_asset_id = ?
        """
        params: list[Any] = [current_id]

        if relationship_types:
            placeholders = ",".join("?" * len(relationship_types))
            query += f" AND r.relationship_type IN ({placeholders})"
            params.extend(relationship_types)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                source_id = row["source_asset_id"]
                if source_id not in visited:
                    queue.append((source_id, depth + 1))

    return result


async def traverse_downstream(
    asset_id: str,
    relationship_types: list[str] | None = None,
    max_depth: int = 5,
) -> dict[str, Any]:
    """
    BFS traversal to find all downstream assets (assets that this one feeds).

    Args:
        asset_id: Starting asset ID
        relationship_types: Filter by specific relationship types
        max_depth: Maximum traversal depth

    Returns:
        Dictionary containing root asset ID, downstream assets with depth info
    """
    db = await get_db()

    visited: set[str] = set()
    result: dict[str, Any] = {
        "root": asset_id,
        "assets": [],
        "depth_map": {},
        "max_depth_reached": max_depth,
    }
    queue: deque[tuple[str, int]] = deque([(asset_id, 0)])

    while queue:
        current_id, depth = queue.popleft()

        if current_id in visited or depth > max_depth:
            continue
        visited.add(current_id)

        # Skip root asset in results
        if current_id != asset_id:
            async with db.execute(
                "SELECT id, name, type, criticality, process_area_id FROM assets WHERE id = ?",
                [current_id],
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    result["assets"].append({
                        "id": row["id"],
                        "name": row["name"],
                        "type": row["type"],
                        "criticality": row["criticality"],
                        "process_area_id": row["process_area_id"],
                        "depth": depth,
                    })
                    result["depth_map"][current_id] = depth

        # Find assets that this one feeds (current -> target)
        query = """
            SELECT r.target_asset_id, r.relationship_type, r.description
            FROM relationships r
            WHERE r.source_asset_id = ?
        """
        params: list[Any] = [current_id]

        if relationship_types:
            placeholders = ",".join("?" * len(relationship_types))
            query += f" AND r.relationship_type IN ({placeholders})"
            params.extend(relationship_types)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                target_id = row["target_asset_id"]
                if target_id not in visited:
                    queue.append((target_id, depth + 1))

    return result


async def find_dependents(
    asset_id: str,
    max_depth: int = 5,
) -> dict[str, Any]:
    """
    Find all assets that depend on this asset (directly or transitively).

    This specifically follows 'depends_on' relationships in reverse
    (finding assets whose operation depends on this one).

    Args:
        asset_id: Asset to find dependents for
        max_depth: Maximum traversal depth

    Returns:
        Dictionary with dependent assets and impact chain
    """
    db = await get_db()

    visited: set[str] = set()
    result: dict[str, Any] = {
        "root": asset_id,
        "dependents": [],
        "impact_chain": [],
    }
    queue: deque[tuple[str, int, list[str]]] = deque([(asset_id, 0, [])])

    while queue:
        current_id, depth, path = queue.popleft()

        if current_id in visited or depth > max_depth:
            continue
        visited.add(current_id)

        current_path = path + [current_id]

        # Skip root
        if current_id != asset_id:
            async with db.execute(
                "SELECT id, name, type, criticality FROM assets WHERE id = ?",
                [current_id],
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    result["dependents"].append({
                        "id": row["id"],
                        "name": row["name"],
                        "type": row["type"],
                        "criticality": row["criticality"],
                        "depth": depth,
                        "dependency_path": current_path,
                    })

        # Find assets that depend on current (source depends_on current)
        async with db.execute(
            """
            SELECT r.source_asset_id
            FROM relationships r
            WHERE r.target_asset_id = ? AND r.relationship_type = 'depends_on'
            """,
            [current_id],
        ) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                source_id = row["source_asset_id"]
                if source_id not in visited:
                    queue.append((source_id, depth + 1, current_path))

    return result


async def check_redundancy(asset_id: str) -> dict[str, Any]:
    """
    Check if an asset has redundancy configured.

    Args:
        asset_id: Asset to check

    Returns:
        Dictionary with redundancy status and details
    """
    db = await get_db()

    # Find redundant_with relationships
    async with db.execute(
        """
        SELECT r.*, a.id as redundant_id, a.name as redundant_name, a.type as redundant_type
        FROM relationships r
        JOIN assets a ON (
            (r.source_asset_id = ? AND r.target_asset_id = a.id) OR
            (r.target_asset_id = ? AND r.source_asset_id = a.id)
        )
        WHERE r.relationship_type = 'redundant_with'
        AND (r.source_asset_id = ? OR r.target_asset_id = ?)
        """,
        [asset_id, asset_id, asset_id, asset_id],
    ) as cursor:
        rows = await cursor.fetchall()
        redundant_assets = []
        for row in rows:
            # Get the other asset in the relationship
            other_id = row["target_asset_id"] if row["source_asset_id"] == asset_id else row["source_asset_id"]
            if other_id != asset_id:
                redundant_assets.append({
                    "id": other_id,
                    "name": row["redundant_name"],
                    "type": row["redundant_type"],
                    "verified": bool(row["verified"]),
                })

    # Also check for backs_up relationships
    async with db.execute(
        """
        SELECT a.id, a.name, a.type
        FROM relationships r
        JOIN assets a ON r.source_asset_id = a.id
        WHERE r.target_asset_id = ? AND r.relationship_type = 'backs_up'
        """,
        [asset_id],
    ) as cursor:
        rows = await cursor.fetchall()
        backup_assets = [{"id": row["id"], "name": row["name"], "type": row["type"]} for row in rows]

    return {
        "asset_id": asset_id,
        "has_redundancy": len(redundant_assets) > 0 or len(backup_assets) > 0,
        "redundant_assets": redundant_assets,
        "backup_assets": backup_assets,
    }


async def get_relationship_graph(
    process_area_id: str | None = None,
    include_types: list[str] | None = None,
) -> dict[str, Any]:
    """
    Get the full relationship graph for visualization.

    Args:
        process_area_id: Filter to specific process area
        include_types: Only include certain relationship types

    Returns:
        Graph structure with nodes (assets) and edges (relationships)
    """
    db = await get_db()

    # Build asset query
    asset_query = "SELECT id, name, type, criticality, process_area_id FROM assets"
    asset_params: list[Any] = []
    if process_area_id:
        asset_query += " WHERE process_area_id = ?"
        asset_params.append(process_area_id)

    async with db.execute(asset_query, asset_params) as cursor:
        rows = await cursor.fetchall()
        nodes = [
            {
                "id": row["id"],
                "name": row["name"],
                "type": row["type"],
                "criticality": row["criticality"],
            }
            for row in rows
        ]
        node_ids = {row["id"] for row in rows}

    # Build relationship query
    rel_query = """
        SELECT id, source_asset_id, target_asset_id, relationship_type, verified
        FROM relationships
    """
    rel_params: list[Any] = []
    conditions = []

    if process_area_id:
        # Only include relationships between assets in the process area
        conditions.append("source_asset_id IN (SELECT id FROM assets WHERE process_area_id = ?)")
        conditions.append("target_asset_id IN (SELECT id FROM assets WHERE process_area_id = ?)")
        rel_params.extend([process_area_id, process_area_id])

    if include_types:
        placeholders = ",".join("?" * len(include_types))
        conditions.append(f"relationship_type IN ({placeholders})")
        rel_params.extend(include_types)

    if conditions:
        rel_query += " WHERE " + " AND ".join(conditions)

    async with db.execute(rel_query, rel_params) as cursor:
        rows = await cursor.fetchall()
        edges = [
            {
                "id": row["id"],
                "source": row["source_asset_id"],
                "target": row["target_asset_id"],
                "type": row["relationship_type"],
                "verified": bool(row["verified"]),
            }
            for row in rows
            if row["source_asset_id"] in node_ids and row["target_asset_id"] in node_ids
        ]

    return {
        "nodes": nodes,
        "edges": edges,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }
