"""Compliance and audit tools for OT Asset Inventory MCP Server."""

from typing import Any
from datetime import date, timedelta

from ..db.connection import get_db


async def find_gaps(
    gap_types: list[str] | None = None,
    process_area: str | None = None,
    criticality: str | None = None,
) -> dict[str, Any]:
    """
    Find assets with compliance or documentation gaps.

    This is essential for audit preparation and identifying areas needing attention.

    Gap types include:
    - no_owner: Assets without an assigned owner
    - not_in_cmms: Assets not registered in CMMS
    - undocumented: Assets lacking documentation
    - no_security_policy: Assets without security policy applied
    - unverified: Assets not verified recently (>6 months)
    - stale_verification: Assets verified >12 months ago

    Args:
        gap_types: Filter to specific gap types (default: all)
        process_area: Filter by process area name or ID
        criticality: Filter by criticality level

    Returns:
        Dictionary with assets grouped by gap type and summary statistics
    """
    db = await get_db()

    # Default to all gap types
    if gap_types is None:
        gap_types = ["no_owner", "not_in_cmms", "undocumented", "no_security_policy", "unverified"]

    results: dict[str, Any] = {}
    six_months_ago = (date.today() - timedelta(days=180)).isoformat()
    twelve_months_ago = (date.today() - timedelta(days=365)).isoformat()

    # Build base query parts
    base_select = """
        SELECT a.id, a.name, a.type, a.criticality, a.process_area_id,
               pa.name as process_area_name,
               a.owner, a.in_cmms, a.documented, a.security_policy_applied, a.last_verified
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        WHERE 1=1
    """
    base_params: list[Any] = []

    if process_area:
        base_select += " AND (a.process_area_id = ? OR pa.name LIKE ?)"
        base_params.extend([process_area, f"%{process_area}%"])

    if criticality:
        base_select += " AND a.criticality = ?"
        base_params.append(criticality)

    # Query each gap type
    if "no_owner" in gap_types:
        query = base_select + " AND a.owner IS NULL ORDER BY a.criticality DESC, a.name"
        async with db.execute(query, base_params) as cursor:
            rows = await cursor.fetchall()
            results["no_owner"] = [_format_gap_asset(row, "No owner assigned") for row in rows]

    if "not_in_cmms" in gap_types:
        query = base_select + " AND NOT a.in_cmms ORDER BY a.criticality DESC, a.name"
        async with db.execute(query, base_params) as cursor:
            rows = await cursor.fetchall()
            results["not_in_cmms"] = [_format_gap_asset(row, "Not registered in CMMS") for row in rows]

    if "undocumented" in gap_types:
        query = base_select + " AND NOT a.documented ORDER BY a.criticality DESC, a.name"
        async with db.execute(query, base_params) as cursor:
            rows = await cursor.fetchall()
            results["undocumented"] = [_format_gap_asset(row, "Missing documentation") for row in rows]

    if "no_security_policy" in gap_types:
        query = base_select + " AND NOT a.security_policy_applied ORDER BY a.criticality DESC, a.name"
        async with db.execute(query, base_params) as cursor:
            rows = await cursor.fetchall()
            results["no_security_policy"] = [_format_gap_asset(row, "Security policy not applied") for row in rows]

    if "unverified" in gap_types:
        query = base_select + " AND (a.last_verified IS NULL OR a.last_verified < ?) ORDER BY a.criticality DESC, a.name"
        params = base_params + [six_months_ago]
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            results["unverified"] = [
                _format_gap_asset(
                    row,
                    "Never verified" if row["last_verified"] is None else f"Last verified: {row['last_verified']}"
                )
                for row in rows
            ]

    if "stale_verification" in gap_types:
        query = base_select + " AND a.last_verified IS NOT NULL AND a.last_verified < ? ORDER BY a.last_verified, a.criticality DESC"
        params = base_params + [twelve_months_ago]
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            results["stale_verification"] = [
                _format_gap_asset(row, f"Verification stale: {row['last_verified']}")
                for row in rows
            ]

    # Calculate summary
    total_gaps = sum(len(v) for v in results.values())
    unique_assets = set()
    for gap_list in results.values():
        for asset in gap_list:
            unique_assets.add(asset["id"])

    # Critical gaps (critical assets with any gap)
    critical_gaps = set()
    for gap_list in results.values():
        for asset in gap_list:
            if asset["criticality"] == "critical":
                critical_gaps.add(asset["id"])

    return {
        "gaps": results,
        "summary": {
            "total_gap_instances": total_gaps,
            "unique_assets_with_gaps": len(unique_assets),
            "critical_assets_with_gaps": len(critical_gaps),
            "gap_counts": {k: len(v) for k, v in results.items()},
        },
    }


async def compare_to_source(
    source_type: str,
    source_data: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Compare inventory against an external data source.

    Useful for reconciliation with CMMS exports, network scans, or manual lists.

    Args:
        source_type: Type of source (cmms, network_scan, manual_list)
        source_data: List of assets from external source with at least 'id' or matching fields

    Returns:
        Comparison showing:
        - in_inventory_only: Assets in our inventory but not in source
        - in_source_only: Assets in source but not in our inventory
        - mismatched: Assets present in both but with differing data
        - matched: Assets that match between both sources
    """
    db = await get_db()

    # Get all assets from inventory
    async with db.execute(
        "SELECT id, name, type, ip_address, manufacturer, model FROM assets"
    ) as cursor:
        rows = await cursor.fetchall()
        inventory_assets = {row["id"]: dict(row) for row in rows}

    # Build source asset lookup
    source_assets: dict[str, dict[str, Any]] = {}
    for asset in source_data:
        # Try to match by ID first, then by IP address
        asset_id = asset.get("id")
        if asset_id:
            source_assets[asset_id] = asset

    inventory_ids = set(inventory_assets.keys())
    source_ids = set(source_assets.keys())

    # Find differences
    in_inventory_only = [
        {"id": aid, "name": inventory_assets[aid].get("name"), "type": inventory_assets[aid].get("type")}
        for aid in inventory_ids - source_ids
    ]

    in_source_only = [
        source_assets[aid]
        for aid in source_ids - inventory_ids
    ]

    # Find mismatches
    common_ids = inventory_ids & source_ids
    mismatched = []
    matched = []

    compare_fields = ["name", "type", "ip_address", "manufacturer", "model"]

    for aid in common_ids:
        inv_asset = inventory_assets[aid]
        src_asset = source_assets[aid]
        differences = []

        for field in compare_fields:
            inv_value = inv_asset.get(field)
            src_value = src_asset.get(field)
            if inv_value != src_value and (inv_value or src_value):
                differences.append({
                    "field": field,
                    "inventory_value": inv_value,
                    "source_value": src_value,
                })

        if differences:
            mismatched.append({
                "id": aid,
                "name": inv_asset.get("name"),
                "differences": differences,
            })
        else:
            matched.append({"id": aid, "name": inv_asset.get("name")})

    return {
        "source_type": source_type,
        "comparison_timestamp": date.today().isoformat(),
        "summary": {
            "inventory_count": len(inventory_assets),
            "source_count": len(source_assets),
            "in_inventory_only": len(in_inventory_only),
            "in_source_only": len(in_source_only),
            "mismatched": len(mismatched),
            "matched": len(matched),
        },
        "in_inventory_only": in_inventory_only,
        "in_source_only": in_source_only,
        "mismatched": mismatched,
        "matched_count": len(matched),
    }


async def audit_summary(
    process_area: str | None = None,
    include_recommendations: bool = True,
) -> dict[str, Any]:
    """
    Generate a comprehensive audit readiness summary.

    This provides a high-level view of inventory health and compliance status,
    suitable for management reporting and audit preparation.

    Args:
        process_area: Filter to specific process area (name or ID)
        include_recommendations: Include actionable recommendations

    Returns:
        Comprehensive audit summary including:
        - Total asset counts by type and criticality
        - Compliance statistics and percentages
        - Gap analysis with counts
        - Critical issues requiring attention
        - Recommendations for improvement
    """
    db = await get_db()

    # Build base query filter
    filter_sql = ""
    filter_params: list[Any] = []
    if process_area:
        filter_sql = " WHERE (a.process_area_id = ? OR pa.name LIKE ?)"
        filter_params = [process_area, f"%{process_area}%"]

    # Total asset count
    async with db.execute(
        f"""
        SELECT COUNT(*) as total FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        {filter_sql}
        """,
        filter_params,
    ) as cursor:
        row = await cursor.fetchone()
        total_assets = row["total"]

    # Count by type
    async with db.execute(
        f"""
        SELECT a.type, COUNT(*) as count FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        {filter_sql}
        GROUP BY a.type ORDER BY count DESC
        """,
        filter_params,
    ) as cursor:
        rows = await cursor.fetchall()
        by_type = {row["type"]: row["count"] for row in rows}

    # Count by criticality
    async with db.execute(
        f"""
        SELECT COALESCE(a.criticality, 'unassigned') as criticality, COUNT(*) as count
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        {filter_sql}
        GROUP BY a.criticality
        """,
        filter_params,
    ) as cursor:
        rows = await cursor.fetchall()
        by_criticality = {row["criticality"]: row["count"] for row in rows}

    # Compliance statistics
    async with db.execute(
        f"""
        SELECT
            SUM(CASE WHEN a.owner IS NOT NULL THEN 1 ELSE 0 END) as has_owner,
            SUM(CASE WHEN a.in_cmms THEN 1 ELSE 0 END) as in_cmms,
            SUM(CASE WHEN a.documented THEN 1 ELSE 0 END) as documented,
            SUM(CASE WHEN a.security_policy_applied THEN 1 ELSE 0 END) as security_policy,
            SUM(CASE WHEN a.last_verified IS NOT NULL THEN 1 ELSE 0 END) as verified
        FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        {filter_sql}
        """,
        filter_params,
    ) as cursor:
        row = await cursor.fetchone()
        compliance_stats = {
            "has_owner": {"count": row["has_owner"], "percentage": _pct(row["has_owner"], total_assets)},
            "in_cmms": {"count": row["in_cmms"], "percentage": _pct(row["in_cmms"], total_assets)},
            "documented": {"count": row["documented"], "percentage": _pct(row["documented"], total_assets)},
            "security_policy_applied": {"count": row["security_policy"], "percentage": _pct(row["security_policy"], total_assets)},
            "verified": {"count": row["verified"], "percentage": _pct(row["verified"], total_assets)},
        }

    # Gap counts
    gap_result = await find_gaps(process_area=process_area)
    gap_counts = gap_result["summary"]["gap_counts"]

    # Critical assets without owner
    async with db.execute(
        f"""
        SELECT COUNT(*) as count FROM assets a
        LEFT JOIN process_areas pa ON a.process_area_id = pa.id
        WHERE a.criticality = 'critical' AND a.owner IS NULL
        {filter_sql.replace('WHERE', 'AND') if filter_sql else ''}
        """,
        filter_params,
    ) as cursor:
        row = await cursor.fetchone()
        critical_without_owner = row["count"]

    # Build result
    result: dict[str, Any] = {
        "audit_date": date.today().isoformat(),
        "scope": process_area or "All process areas",
        "total_assets": total_assets,
        "assets_by_type": by_type,
        "assets_by_criticality": by_criticality,
        "compliance_statistics": compliance_stats,
        "gap_counts": gap_counts,
        "critical_issues": {
            "critical_assets_without_owner": critical_without_owner,
            "unique_assets_with_gaps": gap_result["summary"]["unique_assets_with_gaps"],
            "critical_assets_with_gaps": gap_result["summary"]["critical_assets_with_gaps"],
        },
        "overall_compliance_score": _calculate_compliance_score(compliance_stats),
    }

    if include_recommendations:
        result["recommendations"] = _generate_recommendations(
            compliance_stats,
            gap_counts,
            critical_without_owner,
            total_assets,
        )

    return result


def _format_gap_asset(row: Any, gap_description: str) -> dict[str, Any]:
    """Format asset row for gap reporting."""
    return {
        "id": row["id"],
        "name": row["name"],
        "type": row["type"],
        "criticality": row["criticality"],
        "process_area": row["process_area_name"],
        "gap_description": gap_description,
    }


def _pct(count: int, total: int) -> float:
    """Calculate percentage."""
    if total == 0:
        return 0.0
    return round((count / total) * 100, 1)


def _calculate_compliance_score(stats: dict[str, Any]) -> dict[str, Any]:
    """Calculate overall compliance score."""
    weights = {
        "has_owner": 25,
        "in_cmms": 20,
        "documented": 25,
        "security_policy_applied": 20,
        "verified": 10,
    }

    weighted_score = sum(
        stats[key]["percentage"] * (weight / 100)
        for key, weight in weights.items()
    )

    return {
        "score": round(weighted_score, 1),
        "max_score": 100,
        "grade": _score_to_grade(weighted_score),
    }


def _score_to_grade(score: float) -> str:
    """Convert score to letter grade."""
    if score >= 90:
        return "A"
    elif score >= 80:
        return "B"
    elif score >= 70:
        return "C"
    elif score >= 60:
        return "D"
    return "F"


def _generate_recommendations(
    compliance_stats: dict[str, Any],
    gap_counts: dict[str, int],
    critical_without_owner: int,
    total_assets: int,
) -> list[str]:
    """Generate actionable recommendations based on audit findings."""
    recommendations = []

    # Priority 1: Critical assets without owners
    if critical_without_owner > 0:
        recommendations.append(
            f"URGENT: Assign owners to {critical_without_owner} critical asset(s) without ownership"
        )

    # Priority 2: Low compliance areas
    if compliance_stats["has_owner"]["percentage"] < 80:
        count = total_assets - compliance_stats["has_owner"]["count"]
        recommendations.append(f"Assign owners to {count} asset(s) ({100 - compliance_stats['has_owner']['percentage']:.0f}% missing)")

    if compliance_stats["in_cmms"]["percentage"] < 90:
        count = total_assets - compliance_stats["in_cmms"]["count"]
        recommendations.append(f"Register {count} asset(s) in CMMS ({100 - compliance_stats['in_cmms']['percentage']:.0f}% not registered)")

    if compliance_stats["documented"]["percentage"] < 80:
        count = total_assets - compliance_stats["documented"]["count"]
        recommendations.append(f"Create documentation for {count} asset(s) ({100 - compliance_stats['documented']['percentage']:.0f}% undocumented)")

    if compliance_stats["security_policy_applied"]["percentage"] < 90:
        count = total_assets - compliance_stats["security_policy_applied"]["count"]
        recommendations.append(f"Apply security policies to {count} asset(s) ({100 - compliance_stats['security_policy_applied']['percentage']:.0f}% without policy)")

    if compliance_stats["verified"]["percentage"] < 70:
        count = total_assets - compliance_stats["verified"]["count"]
        recommendations.append(f"Schedule verification for {count} asset(s) ({100 - compliance_stats['verified']['percentage']:.0f}% not recently verified)")

    if not recommendations:
        recommendations.append("Good compliance posture - maintain current processes")

    return recommendations
