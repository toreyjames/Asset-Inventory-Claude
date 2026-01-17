"""Review and human-in-the-loop tools for OT Asset Inventory MCP Server."""

from typing import Any
from datetime import datetime
import uuid

from ..db.connection import get_db


async def suggest_relationship(
    source_asset_id: str,
    target_asset_id: str,
    relationship_type: str,
    reasoning: str,
) -> dict[str, Any]:
    """
    Suggest a new relationship between assets for human review.

    This allows Claude to propose relationships it infers from context,
    which are then flagged for human verification before being confirmed.

    Args:
        source_asset_id: The source asset in the relationship
        target_asset_id: The target asset in the relationship
        relationship_type: Type of relationship (feeds_data_to, controls, depends_on, etc.)
        reasoning: Explanation for why this relationship is being suggested

    Returns:
        Confirmation of the suggestion with review flag details
    """
    db = await get_db()

    # Validate assets exist
    async with db.execute(
        "SELECT id, name, type FROM assets WHERE id = ?",
        [source_asset_id],
    ) as cursor:
        source = await cursor.fetchone()
        if not source:
            return {"error": f"Source asset {source_asset_id} not found"}

    async with db.execute(
        "SELECT id, name, type FROM assets WHERE id = ?",
        [target_asset_id],
    ) as cursor:
        target = await cursor.fetchone()
        if not target:
            return {"error": f"Target asset {target_asset_id} not found"}

    # Check if relationship already exists
    async with db.execute(
        """
        SELECT id FROM relationships
        WHERE source_asset_id = ? AND target_asset_id = ? AND relationship_type = ?
        """,
        [source_asset_id, target_asset_id, relationship_type],
    ) as cursor:
        existing = await cursor.fetchone()
        if existing:
            return {
                "status": "already_exists",
                "message": "This relationship already exists",
                "relationship_id": existing["id"],
            }

    # Create the relationship as inferred/unverified
    relationship_id = str(uuid.uuid4())
    await db.execute(
        """
        INSERT INTO relationships (id, source_asset_id, target_asset_id, relationship_type, inferred, verified, description)
        VALUES (?, ?, ?, ?, 1, 0, ?)
        """,
        [relationship_id, source_asset_id, target_asset_id, relationship_type, f"AI suggested: {reasoning}"],
    )

    # Create a review flag
    flag_id = str(uuid.uuid4())
    await db.execute(
        """
        INSERT INTO review_flags (id, relationship_id, flag_type, description, severity, flagged_by)
        VALUES (?, ?, 'suggested_relationship', ?, 'medium', 'claude')
        """,
        [flag_id, relationship_id, f"Suggested {relationship_type} relationship: {source['name']} -> {target['name']}. Reasoning: {reasoning}"],
    )

    await db.commit()

    return {
        "status": "suggested",
        "relationship_id": relationship_id,
        "flag_id": flag_id,
        "source": {"id": source["id"], "name": source["name"], "type": source["type"]},
        "target": {"id": target["id"], "name": target["name"], "type": target["type"]},
        "relationship_type": relationship_type,
        "message": "Relationship suggested and flagged for human review",
    }


async def flag_for_review(
    asset_id: str,
    flag_type: str,
    description: str,
    severity: str = "medium",
) -> dict[str, Any]:
    """
    Flag an asset for human review.

    Use this when Claude identifies potential issues that need human verification,
    such as missing data, potential misconfigurations, or compliance concerns.

    Args:
        asset_id: Asset to flag
        flag_type: Type of flag:
            - missing_data: Asset is missing important information
            - needs_verification: Asset data needs to be verified
            - potential_issue: Potential problem identified
            - compliance_gap: Compliance concern
            - ownership_unknown: Owner should be assigned
        description: Detailed description of the issue
        severity: Severity level (critical, high, medium, low)

    Returns:
        Confirmation of the flag creation
    """
    db = await get_db()

    # Validate asset exists
    async with db.execute(
        "SELECT id, name, type FROM assets WHERE id = ?",
        [asset_id],
    ) as cursor:
        asset = await cursor.fetchone()
        if not asset:
            return {"error": f"Asset {asset_id} not found"}

    # Validate flag type
    valid_flag_types = [
        "missing_data", "needs_verification", "potential_issue",
        "suggested_relationship", "compliance_gap", "ownership_unknown"
    ]
    if flag_type not in valid_flag_types:
        return {"error": f"Invalid flag type. Must be one of: {valid_flag_types}"}

    # Validate severity
    valid_severities = ["critical", "high", "medium", "low"]
    if severity not in valid_severities:
        severity = "medium"

    # Create the flag
    flag_id = str(uuid.uuid4())
    await db.execute(
        """
        INSERT INTO review_flags (id, asset_id, flag_type, description, severity, flagged_by)
        VALUES (?, ?, ?, ?, ?, 'claude')
        """,
        [flag_id, asset_id, flag_type, description, severity],
    )

    await db.commit()

    return {
        "status": "flagged",
        "flag_id": flag_id,
        "asset": {"id": asset["id"], "name": asset["name"], "type": asset["type"]},
        "flag_type": flag_type,
        "severity": severity,
        "description": description,
        "message": "Asset flagged for human review",
    }


async def list_review_flags(
    status: str = "open",
    flag_type: str | None = None,
    asset_id: str | None = None,
    severity: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    List review flags with optional filtering.

    Args:
        status: Filter by status (open, in_review, resolved, dismissed)
        flag_type: Filter by flag type
        asset_id: Filter by asset
        severity: Filter by severity
        limit: Maximum results

    Returns:
        List of review flags with asset details
    """
    db = await get_db()

    query = """
        SELECT rf.*,
               a.name as asset_name, a.type as asset_type,
               r.source_asset_id, r.target_asset_id, r.relationship_type
        FROM review_flags rf
        LEFT JOIN assets a ON rf.asset_id = a.id
        LEFT JOIN relationships r ON rf.relationship_id = r.id
        WHERE rf.status = ?
    """
    params: list[Any] = [status]

    if flag_type:
        query += " AND rf.flag_type = ?"
        params.append(flag_type)

    if asset_id:
        query += " AND rf.asset_id = ?"
        params.append(asset_id)

    if severity:
        query += " AND rf.severity = ?"
        params.append(severity)

    query += f"""
        ORDER BY
            CASE rf.severity
                WHEN 'critical' THEN 1
                WHEN 'high' THEN 2
                WHEN 'medium' THEN 3
                WHEN 'low' THEN 4
            END,
            rf.flagged_at DESC
        LIMIT {min(limit, 200)}
    """

    async with db.execute(query, params) as cursor:
        rows = await cursor.fetchall()
        return [
            {
                "id": row["id"],
                "flag_type": row["flag_type"],
                "description": row["description"],
                "severity": row["severity"],
                "status": row["status"],
                "flagged_by": row["flagged_by"],
                "flagged_at": row["flagged_at"],
                "asset": {
                    "id": row["asset_id"],
                    "name": row["asset_name"],
                    "type": row["asset_type"],
                } if row["asset_id"] else None,
                "relationship": {
                    "id": row["relationship_id"],
                    "source_id": row["source_asset_id"],
                    "target_id": row["target_asset_id"],
                    "type": row["relationship_type"],
                } if row["relationship_id"] else None,
            }
            for row in rows
        ]


async def resolve_flag(
    flag_id: str,
    resolution: str,
    resolved_by: str = "user",
    notes: str | None = None,
) -> dict[str, Any]:
    """
    Resolve a review flag.

    Args:
        flag_id: The flag to resolve
        resolution: Resolution status (resolved, dismissed)
        resolved_by: Who resolved the flag
        notes: Optional resolution notes

    Returns:
        Confirmation of resolution
    """
    db = await get_db()

    # Check flag exists
    async with db.execute(
        "SELECT * FROM review_flags WHERE id = ?",
        [flag_id],
    ) as cursor:
        flag = await cursor.fetchone()
        if not flag:
            return {"error": f"Flag {flag_id} not found"}

    if flag["status"] != "open" and flag["status"] != "in_review":
        return {"error": f"Flag is already {flag['status']}"}

    # Update the flag
    await db.execute(
        """
        UPDATE review_flags
        SET status = ?, resolved_by = ?, resolved_at = ?, resolution_notes = ?
        WHERE id = ?
        """,
        [resolution, resolved_by, datetime.now().isoformat(), notes, flag_id],
    )

    # If this was a relationship suggestion and it's resolved, verify the relationship
    if flag["flag_type"] == "suggested_relationship" and resolution == "resolved" and flag["relationship_id"]:
        await db.execute(
            """
            UPDATE relationships
            SET verified = 1, verified_by = ?, verified_at = ?
            WHERE id = ?
            """,
            [resolved_by, datetime.now().isoformat(), flag["relationship_id"]],
        )

    await db.commit()

    return {
        "status": "success",
        "flag_id": flag_id,
        "resolution": resolution,
        "resolved_by": resolved_by,
        "message": f"Flag {resolution}",
    }


async def get_review_summary() -> dict[str, Any]:
    """
    Get a summary of all review flags.

    Returns:
        Summary statistics for review flags
    """
    db = await get_db()

    # Count by status
    async with db.execute(
        """
        SELECT status, COUNT(*) as count
        FROM review_flags
        GROUP BY status
        """
    ) as cursor:
        rows = await cursor.fetchall()
        by_status = {row["status"]: row["count"] for row in rows}

    # Count open flags by severity
    async with db.execute(
        """
        SELECT severity, COUNT(*) as count
        FROM review_flags
        WHERE status = 'open'
        GROUP BY severity
        """
    ) as cursor:
        rows = await cursor.fetchall()
        open_by_severity = {row["severity"]: row["count"] for row in rows}

    # Count open flags by type
    async with db.execute(
        """
        SELECT flag_type, COUNT(*) as count
        FROM review_flags
        WHERE status = 'open'
        GROUP BY flag_type
        """
    ) as cursor:
        rows = await cursor.fetchall()
        open_by_type = {row["flag_type"]: row["count"] for row in rows}

    # Get oldest open flag
    async with db.execute(
        """
        SELECT flagged_at FROM review_flags
        WHERE status = 'open'
        ORDER BY flagged_at ASC
        LIMIT 1
        """
    ) as cursor:
        row = await cursor.fetchone()
        oldest_open = row["flagged_at"] if row else None

    return {
        "total_flags": sum(by_status.values()),
        "by_status": by_status,
        "open_flags": {
            "total": by_status.get("open", 0),
            "by_severity": open_by_severity,
            "by_type": open_by_type,
        },
        "oldest_open_flag": oldest_open,
        "requires_attention": open_by_severity.get("critical", 0) + open_by_severity.get("high", 0),
    }
