"""Impact analysis tools for OT Asset Inventory MCP Server."""

from typing import Any

from ..db.connection import get_db
from ..utils.graph import traverse_downstream, find_dependents, check_redundancy


async def analyze_impact(
    asset_id: str,
    failure_type: str = "complete",
) -> dict[str, Any]:
    """
    Analyze the impact if the specified asset fails or goes offline.

    This is a critical tool for understanding risk and planning maintenance.
    It traces through the relationship graph to identify all affected systems.

    Args:
        asset_id: Asset to analyze (e.g., "PLC-101")
        failure_type: Type of failure scenario:
            - "complete": Total asset failure
            - "degraded": Partial functionality loss
            - "intermittent": Sporadic failures

    Returns:
        Impact analysis including:
        - failing_asset: Details of the asset being analyzed
        - directly_affected: Assets immediately impacted
        - cascade_effects: Assets impacted through dependency chain
        - affected_process_areas: Process areas that would be impacted
        - criticality_summary: Breakdown by criticality level
        - safety_implications: Whether safety systems are affected
        - recommendations: Suggested mitigations
    """
    db = await get_db()

    # Get the failing asset details
    async with db.execute(
        """
        SELECT a.*, pa.name as process_area_name
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        WHERE a.id = ?
        """,
        [asset_id],
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return {"error": f"Asset {asset_id} not found"}

        failing_asset = {
            "id": row["id"],
            "name": row["name"],
            "type": row["type"],
            "criticality": row["criticality"],
            "process_area": row["process_area_name"],
            "function": row["function"],
        }

    # Find directly affected assets (immediate downstream)
    async with db.execute(
        """
        SELECT a.id, a.name, a.type, a.criticality, a.process_area_id,
               pa.name as process_area_name, r.relationship_type, r.description
        FROM relationships r
        JOIN assets a ON r.target_asset_id = a.id
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        WHERE r.source_asset_id = ?
        """,
        [asset_id],
    ) as cursor:
        rows = await cursor.fetchall()
        directly_affected = [
            {
                "id": row["id"],
                "name": row["name"],
                "type": row["type"],
                "criticality": row["criticality"],
                "process_area": row["process_area_name"],
                "impact_type": row["relationship_type"],
                "description": row["description"],
            }
            for row in rows
        ]

    # Find cascade effects through dependency chain
    dependents = await find_dependents(asset_id, max_depth=5)
    cascade_effects = [
        {
            "id": dep["id"],
            "name": dep["name"],
            "type": dep["type"],
            "criticality": dep["criticality"],
            "dependency_depth": dep["depth"],
            "dependency_path": " → ".join(dep["dependency_path"]),
        }
        for dep in dependents["dependents"]
    ]

    # Get process areas affected
    affected_ids = {asset_id} | {a["id"] for a in directly_affected} | {a["id"] for a in cascade_effects}
    process_areas: set[str] = set()

    if affected_ids:
        placeholders = ",".join("?" * len(affected_ids))
        async with db.execute(
            f"""
            SELECT DISTINCT pa.name
            FROM assets a
            JOIN process_areas pa ON a.process_area_id = pa.id
            WHERE a.id IN ({placeholders})
            """,
            list(affected_ids),
        ) as cursor:
            rows = await cursor.fetchall()
            process_areas = {row["name"] for row in rows}

    # Check for safety implications
    safety_affected = False
    async with db.execute(
        """
        SELECT COUNT(*) as count
        FROM relationships r
        WHERE r.source_asset_id = ? AND r.relationship_type = 'safety_interlock_for'
        """,
        [asset_id],
    ) as cursor:
        row = await cursor.fetchone()
        safety_affected = row["count"] > 0

    # Also check if any affected asset has safety role
    if not safety_affected and affected_ids:
        placeholders = ",".join("?" * len(affected_ids))
        async with db.execute(
            f"""
            SELECT COUNT(*) as count FROM relationships
            WHERE source_asset_id IN ({placeholders})
            AND relationship_type = 'safety_interlock_for'
            """,
            list(affected_ids),
        ) as cursor:
            row = await cursor.fetchone()
            safety_affected = row["count"] > 0

    # Check redundancy
    redundancy = await check_redundancy(asset_id)

    # Summarize criticality
    all_affected = directly_affected + cascade_effects
    criticality_summary = {
        "critical": len([a for a in all_affected if a.get("criticality") == "critical"]),
        "high": len([a for a in all_affected if a.get("criticality") == "high"]),
        "medium": len([a for a in all_affected if a.get("criticality") == "medium"]),
        "low": len([a for a in all_affected if a.get("criticality") == "low"]),
    }

    # Generate recommendations
    recommendations = []
    if not redundancy["has_redundancy"]:
        recommendations.append(f"CRITICAL: {failing_asset['name']} has no redundancy configured")
    if criticality_summary["critical"] > 0:
        recommendations.append(
            f"Failure would affect {criticality_summary['critical']} critical asset(s)"
        )
    if safety_affected:
        recommendations.append("WARNING: Safety systems may be affected - review safety protocols")
    if len(process_areas) > 1:
        recommendations.append(f"Impact spans {len(process_areas)} process areas - coordinate response")

    return {
        "failing_asset": failing_asset,
        "failure_type": failure_type,
        "directly_affected": directly_affected,
        "directly_affected_count": len(directly_affected),
        "cascade_effects": cascade_effects,
        "cascade_count": len(cascade_effects),
        "total_affected": len(all_affected),
        "affected_process_areas": list(process_areas),
        "criticality_summary": criticality_summary,
        "safety_implications": safety_affected,
        "has_redundancy": redundancy["has_redundancy"],
        "redundancy_details": redundancy,
        "recommendations": recommendations,
    }


async def find_single_points_of_failure(
    process_area: str | None = None,
    criticality_threshold: str = "high",
) -> list[dict[str, Any]]:
    """
    Identify assets that are single points of failure (SPOF).

    A SPOF is an asset where:
    - It has no redundancy configured (no redundant_with or backs_up relationships)
    - Multiple assets depend on it OR critical assets depend on it
    - Its failure would cause significant impact

    Args:
        process_area: Limit analysis to specific process area (name or ID)
        criticality_threshold: Minimum criticality to consider ("critical", "high", "medium", "low")

    Returns:
        List of SPOFs with impact assessment, sorted by risk level
    """
    db = await get_db()

    criticality_levels = ["critical", "high", "medium", "low"]
    threshold_index = criticality_levels.index(criticality_threshold) if criticality_threshold in criticality_levels else 1
    included_levels = criticality_levels[: threshold_index + 1]

    # Build query for assets at or above threshold
    query = """
        SELECT a.id, a.name, a.type, a.criticality, a.process_area_id,
               pa.name as process_area_name
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        WHERE a.criticality IN ({})
    """.format(",".join("?" * len(included_levels)))
    params: list[Any] = list(included_levels)

    if process_area:
        query += " AND (a.process_area_id = ? OR pa.name LIKE ?)"
        params.extend([process_area, f"%{process_area}%"])

    spofs = []

    async with db.execute(query, params) as cursor:
        rows = await cursor.fetchall()

        for row in rows:
            asset_id = row["id"]

            # Check redundancy
            redundancy = await check_redundancy(asset_id)
            if redundancy["has_redundancy"]:
                continue  # Has redundancy, not a SPOF

            # Find assets that depend on this one
            dependents = await find_dependents(asset_id, max_depth=3)
            dependent_count = len(dependents["dependents"])

            # Count direct downstream relationships
            async with db.execute(
                "SELECT COUNT(*) as count FROM relationships WHERE source_asset_id = ?",
                [asset_id],
            ) as rel_cursor:
                rel_row = await rel_cursor.fetchone()
                downstream_count = rel_row["count"]

            # Count critical dependents
            critical_dependents = len([d for d in dependents["dependents"] if d.get("criticality") == "critical"])

            # Determine if this is a SPOF (dependent count > 0 or multiple downstream)
            if dependent_count > 0 or downstream_count > 2:
                # Calculate risk score
                risk_score = _calculate_spof_risk(
                    asset_criticality=row["criticality"],
                    dependent_count=dependent_count,
                    critical_dependents=critical_dependents,
                    downstream_count=downstream_count,
                )

                spofs.append({
                    "id": row["id"],
                    "name": row["name"],
                    "type": row["type"],
                    "criticality": row["criticality"],
                    "process_area": row["process_area_name"],
                    "dependent_count": dependent_count,
                    "critical_dependents": critical_dependents,
                    "downstream_count": downstream_count,
                    "risk_score": risk_score,
                    "risk_level": _risk_level(risk_score),
                    "recommendation": _spof_recommendation(row, dependent_count, critical_dependents),
                })

    # Sort by risk score descending
    spofs.sort(key=lambda x: x["risk_score"], reverse=True)

    return spofs


async def get_critical_path(
    source_asset_id: str,
    target_asset_id: str,
) -> dict[str, Any]:
    """
    Find the critical path between two assets.

    Args:
        source_asset_id: Starting asset
        target_asset_id: Ending asset

    Returns:
        Path information including all assets and relationships in the path
    """
    db = await get_db()
    from collections import deque

    # BFS to find shortest path
    visited: set[str] = set()
    queue: deque[tuple[str, list[str]]] = deque([(source_asset_id, [source_asset_id])])

    while queue:
        current_id, path = queue.popleft()

        if current_id == target_asset_id:
            # Found the path - get details
            path_details = []
            for i, asset_id in enumerate(path):
                async with db.execute(
                    "SELECT id, name, type, criticality FROM assets WHERE id = ?",
                    [asset_id],
                ) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        asset_detail = {
                            "id": row["id"],
                            "name": row["name"],
                            "type": row["type"],
                            "criticality": row["criticality"],
                            "position": i,
                        }
                        # Get relationship to next asset if not last
                        if i < len(path) - 1:
                            next_id = path[i + 1]
                            async with db.execute(
                                """
                                SELECT relationship_type, description
                                FROM relationships
                                WHERE source_asset_id = ? AND target_asset_id = ?
                                """,
                                [asset_id, next_id],
                            ) as rel_cursor:
                                rel_row = await rel_cursor.fetchone()
                                if rel_row:
                                    asset_detail["relationship_to_next"] = rel_row["relationship_type"]

                        path_details.append(asset_detail)

            return {
                "found": True,
                "path_length": len(path),
                "path": path_details,
            }

        if current_id in visited:
            continue
        visited.add(current_id)

        # Get all connected assets
        async with db.execute(
            """
            SELECT target_asset_id as next_id FROM relationships WHERE source_asset_id = ?
            UNION
            SELECT source_asset_id as next_id FROM relationships WHERE target_asset_id = ?
            """,
            [current_id, current_id],
        ) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                next_id = row["next_id"]
                if next_id not in visited:
                    queue.append((next_id, path + [next_id]))

    return {
        "found": False,
        "message": f"No path found between {source_asset_id} and {target_asset_id}",
    }


def _calculate_spof_risk(
    asset_criticality: str | None,
    dependent_count: int,
    critical_dependents: int,
    downstream_count: int,
) -> float:
    """Calculate risk score for a potential SPOF."""
    base_score = {
        "critical": 100,
        "high": 75,
        "medium": 50,
        "low": 25,
    }.get(asset_criticality or "low", 25)

    # Add for dependents
    base_score += dependent_count * 10
    base_score += critical_dependents * 25
    base_score += downstream_count * 5

    return min(base_score, 200)  # Cap at 200


def _risk_level(score: float) -> str:
    """Convert risk score to level."""
    if score >= 150:
        return "critical"
    elif score >= 100:
        return "high"
    elif score >= 50:
        return "medium"
    return "low"


def _spof_recommendation(asset: Any, dependent_count: int, critical_dependents: int) -> str:
    """Generate recommendation for SPOF."""
    if critical_dependents > 0:
        return f"URGENT: Add redundancy - {critical_dependents} critical asset(s) depend on this"
    if dependent_count > 3:
        return f"HIGH PRIORITY: Multiple systems ({dependent_count}) depend on this asset"
    if asset["criticality"] == "critical":
        return "Critical asset without redundancy - evaluate backup options"
    return "Consider adding redundancy or backup procedures"
