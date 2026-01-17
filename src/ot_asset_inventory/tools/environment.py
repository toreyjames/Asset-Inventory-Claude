"""Environment and process area tools for OT Asset Inventory MCP Server."""

from typing import Any

from ..db.connection import get_db


async def get_environment(environment_id: str) -> dict[str, Any] | None:
    """
    Get detailed information about an environment including its sites and process areas.

    Args:
        environment_id: The environment ID to look up

    Returns:
        Environment details with nested sites and process areas, or None if not found
    """
    db = await get_db()

    # Get environment
    async with db.execute(
        "SELECT * FROM environments WHERE id = ?",
        [environment_id],
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None

        environment = {
            "id": row["id"],
            "name": row["name"],
            "type": row["type"],
            "description": row["description"],
        }

    # Get sites
    async with db.execute(
        "SELECT * FROM sites WHERE environment_id = ?",
        [environment_id],
    ) as cursor:
        rows = await cursor.fetchall()
        sites = []
        for site_row in rows:
            site = {
                "id": site_row["id"],
                "name": site_row["name"],
                "address": site_row["address"],
                "timezone": site_row["timezone"],
                "process_areas": [],
            }

            # Get process areas for this site
            async with db.execute(
                "SELECT * FROM process_areas WHERE site_id = ?",
                [site_row["id"]],
            ) as pa_cursor:
                pa_rows = await pa_cursor.fetchall()
                site["process_areas"] = [
                    {
                        "id": pa["id"],
                        "name": pa["name"],
                        "description": pa["description"],
                        "function": pa["function"],
                    }
                    for pa in pa_rows
                ]

            sites.append(site)

        environment["sites"] = sites

    # Get compliance frameworks
    async with db.execute(
        "SELECT * FROM compliance_frameworks WHERE environment_id = ?",
        [environment_id],
    ) as cursor:
        rows = await cursor.fetchall()
        environment["compliance_frameworks"] = [
            {
                "id": row["id"],
                "name": row["name"],
                "version": row["version"],
                "description": row["description"],
            }
            for row in rows
        ]

    # Get asset counts
    async with db.execute(
        """
        SELECT COUNT(*) as count FROM assets a
        JOIN sites s ON a.site_id = s.id
        WHERE s.environment_id = ?
        """,
        [environment_id],
    ) as cursor:
        row = await cursor.fetchone()
        environment["total_assets"] = row["count"]

    return environment


async def list_environments() -> list[dict[str, Any]]:
    """
    List all environments with summary information.

    Returns:
        List of environments with site and asset counts
    """
    db = await get_db()

    async with db.execute("SELECT * FROM environments ORDER BY name") as cursor:
        rows = await cursor.fetchall()
        environments = []

        for row in rows:
            env = {
                "id": row["id"],
                "name": row["name"],
                "type": row["type"],
                "description": row["description"],
            }

            # Count sites
            async with db.execute(
                "SELECT COUNT(*) as count FROM sites WHERE environment_id = ?",
                [row["id"]],
            ) as site_cursor:
                site_row = await site_cursor.fetchone()
                env["site_count"] = site_row["count"]

            # Count assets
            async with db.execute(
                """
                SELECT COUNT(*) as count FROM assets a
                JOIN sites s ON a.site_id = s.id
                WHERE s.environment_id = ?
                """,
                [row["id"]],
            ) as asset_cursor:
                asset_row = await asset_cursor.fetchone()
                env["asset_count"] = asset_row["count"]

            environments.append(env)

        return environments


async def list_process_areas(
    site_id: str | None = None,
    include_asset_counts: bool = True,
) -> list[dict[str, Any]]:
    """
    List process areas with optional asset counts.

    Args:
        site_id: Filter to a specific site (name or ID)
        include_asset_counts: Include count of assets in each process area

    Returns:
        List of process areas with optional asset counts and criticality breakdown
    """
    db = await get_db()

    query = """
        SELECT pa.*, s.name as site_name
        FROM process_areas pa
        JOIN sites s ON pa.site_id = s.id
    """
    params: list[Any] = []

    if site_id:
        query += " WHERE (pa.site_id = ? OR s.name LIKE ?)"
        params.extend([site_id, f"%{site_id}%"])

    query += " ORDER BY s.name, pa.name"

    async with db.execute(query, params) as cursor:
        rows = await cursor.fetchall()
        process_areas = []

        for row in rows:
            pa: dict[str, Any] = {
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "function": row["function"],
                "site_id": row["site_id"],
                "site_name": row["site_name"],
            }

            if include_asset_counts:
                # Get asset count
                async with db.execute(
                    "SELECT COUNT(*) as count FROM assets WHERE process_area_id = ?",
                    [row["id"]],
                ) as count_cursor:
                    count_row = await count_cursor.fetchone()
                    pa["asset_count"] = count_row["count"]

                # Get criticality breakdown
                async with db.execute(
                    """
                    SELECT criticality, COUNT(*) as count
                    FROM assets
                    WHERE process_area_id = ?
                    GROUP BY criticality
                    """,
                    [row["id"]],
                ) as crit_cursor:
                    crit_rows = await crit_cursor.fetchall()
                    pa["criticality_breakdown"] = {
                        r["criticality"] or "unassigned": r["count"]
                        for r in crit_rows
                    }

                # Get asset type breakdown
                async with db.execute(
                    """
                    SELECT type, COUNT(*) as count
                    FROM assets
                    WHERE process_area_id = ?
                    GROUP BY type
                    """,
                    [row["id"]],
                ) as type_cursor:
                    type_rows = await type_cursor.fetchall()
                    pa["type_breakdown"] = {r["type"]: r["count"] for r in type_rows}

            process_areas.append(pa)

        return process_areas


async def get_process_area(process_area_id: str) -> dict[str, Any] | None:
    """
    Get detailed information about a specific process area.

    Args:
        process_area_id: The process area ID

    Returns:
        Process area details including all assets, or None if not found
    """
    db = await get_db()

    async with db.execute(
        """
        SELECT pa.*, s.name as site_name, e.name as environment_name
        FROM process_areas pa
        JOIN sites s ON pa.site_id = s.id
        JOIN environments e ON s.environment_id = e.id
        WHERE pa.id = ?
        """,
        [process_area_id],
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None

        process_area: dict[str, Any] = {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "function": row["function"],
            "site_id": row["site_id"],
            "site_name": row["site_name"],
            "environment_name": row["environment_name"],
        }

    # Get all assets in this process area
    async with db.execute(
        """
        SELECT id, name, type, criticality, owner, ip_address
        FROM assets
        WHERE process_area_id = ?
        ORDER BY criticality DESC, type, name
        """,
        [process_area_id],
    ) as cursor:
        rows = await cursor.fetchall()
        process_area["assets"] = [
            {
                "id": r["id"],
                "name": r["name"],
                "type": r["type"],
                "criticality": r["criticality"],
                "owner": r["owner"],
                "ip_address": r["ip_address"],
            }
            for r in rows
        ]
        process_area["asset_count"] = len(process_area["assets"])

    # Get compliance statistics for this process area
    async with db.execute(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN owner IS NOT NULL THEN 1 ELSE 0 END) as has_owner,
            SUM(CASE WHEN in_cmms THEN 1 ELSE 0 END) as in_cmms,
            SUM(CASE WHEN documented THEN 1 ELSE 0 END) as documented,
            SUM(CASE WHEN security_policy_applied THEN 1 ELSE 0 END) as has_security
        FROM assets
        WHERE process_area_id = ?
        """,
        [process_area_id],
    ) as cursor:
        row = await cursor.fetchone()
        total = row["total"] or 0
        process_area["compliance_summary"] = {
            "total_assets": total,
            "with_owner": row["has_owner"] or 0,
            "in_cmms": row["in_cmms"] or 0,
            "documented": row["documented"] or 0,
            "with_security_policy": row["has_security"] or 0,
            "ownership_percentage": round((row["has_owner"] or 0) / total * 100, 1) if total > 0 else 0,
            "documentation_percentage": round((row["documented"] or 0) / total * 100, 1) if total > 0 else 0,
        }

    return process_area


async def get_site(site_id: str) -> dict[str, Any] | None:
    """
    Get detailed information about a site.

    Args:
        site_id: The site ID

    Returns:
        Site details including process areas and asset summary
    """
    db = await get_db()

    async with db.execute(
        """
        SELECT s.*, e.name as environment_name
        FROM sites s
        JOIN environments e ON s.environment_id = e.id
        WHERE s.id = ?
        """,
        [site_id],
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None

        site: dict[str, Any] = {
            "id": row["id"],
            "name": row["name"],
            "address": row["address"],
            "timezone": row["timezone"],
            "environment_id": row["environment_id"],
            "environment_name": row["environment_name"],
        }

    # Get process areas
    process_areas = await list_process_areas(site_id=site_id)
    site["process_areas"] = process_areas
    site["process_area_count"] = len(process_areas)

    # Get total asset count
    async with db.execute(
        "SELECT COUNT(*) as count FROM assets WHERE site_id = ?",
        [site_id],
    ) as cursor:
        row = await cursor.fetchone()
        site["total_assets"] = row["count"]

    return site
