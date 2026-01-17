"""MCP Server for OT Asset Inventory.

This server exposes OT asset inventory data to Claude, enabling natural language
queries for audit readiness, gap analysis, and impact/risk assessment.
"""

import asyncio
import json
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from .config import get_config
from .db.connection import DatabaseManager, set_db_manager
from .db.schema import create_tables
from .db.seed import seed_sample_data

# Import tool functions
from .tools import assets, relationships, analysis, compliance, environment, review


# Create the MCP server
server = Server("ot-asset-inventory")


# Tool definitions
TOOLS = [
    # Priority 1: Core Asset Tools
    Tool(
        name="list_assets",
        description="""List OT assets with optional filtering. Use this to find assets by type, location, criticality, or compliance status.

Examples:
- "What PLCs do we have?" -> list_assets(asset_type="PLC")
- "Show critical assets in the cooling system" -> list_assets(process_area="Cooling System", criticality="critical")
- "Which assets have compliance gaps?" -> list_assets(has_gaps=true)""",
        inputSchema={
            "type": "object",
            "properties": {
                "asset_type": {
                    "type": "string",
                    "description": "Filter by asset type (PLC, HMI, Sensor, Actuator, RTU, Gateway, Switch, Server)",
                    "enum": ["PLC", "HMI", "Sensor", "Actuator", "RTU", "Gateway", "Switch", "Server", "Workstation"],
                },
                "process_area": {
                    "type": "string",
                    "description": "Filter by process area name or ID",
                },
                "site": {
                    "type": "string",
                    "description": "Filter by site name or ID",
                },
                "criticality": {
                    "type": "string",
                    "description": "Filter by criticality level",
                    "enum": ["critical", "high", "medium", "low"],
                },
                "owner": {
                    "type": "string",
                    "description": "Filter by owner name",
                },
                "has_gaps": {
                    "type": "boolean",
                    "description": "Only return assets with compliance gaps (missing owner, CMMS, docs, or security policy)",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results to return (default 50, max 100)",
                    "default": 50,
                },
            },
        },
    ),
    Tool(
        name="get_asset",
        description="""Get detailed information about a specific asset including its relationships, compliance status, and any open review flags.

Example: get_asset(asset_id="PLC-101")""",
        inputSchema={
            "type": "object",
            "properties": {
                "asset_id": {
                    "type": "string",
                    "description": "The unique identifier of the asset (e.g., PLC-101)",
                },
            },
            "required": ["asset_id"],
        },
    ),
    Tool(
        name="search_assets",
        description="""Search assets by text query across multiple fields (name, manufacturer, model, notes, function).

Example: "Find anything related to chiller" -> search_assets(query="chiller")""",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search text",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum results (default 20)",
                    "default": 20,
                },
            },
            "required": ["query"],
        },
    ),

    # Priority 2: Relationship Tools
    Tool(
        name="get_upstream",
        description="""Get all assets upstream of the specified asset - i.e., assets that feed data into it.

Example: "What feeds data to PLC-101?" -> get_upstream(asset_id="PLC-101")""",
        inputSchema={
            "type": "object",
            "properties": {
                "asset_id": {
                    "type": "string",
                    "description": "Starting asset ID",
                },
                "relationship_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by relationship types (e.g., feeds_data_to, monitors)",
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum traversal depth (default 5)",
                    "default": 5,
                },
            },
            "required": ["asset_id"],
        },
    ),
    Tool(
        name="get_downstream",
        description="""Get all assets downstream of the specified asset - i.e., assets that it feeds data to or controls.

Example: "What does PLC-101 control?" -> get_downstream(asset_id="PLC-101")""",
        inputSchema={
            "type": "object",
            "properties": {
                "asset_id": {
                    "type": "string",
                    "description": "Starting asset ID",
                },
                "relationship_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by relationship types (e.g., controls, feeds_data_to)",
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum traversal depth (default 5)",
                    "default": 5,
                },
            },
            "required": ["asset_id"],
        },
    ),
    Tool(
        name="get_dependencies",
        description="""Get complete dependency map for an asset - both what it depends on and what depends on it.

Example: "Show all dependencies for PLC-101" -> get_dependencies(asset_id="PLC-101")""",
        inputSchema={
            "type": "object",
            "properties": {
                "asset_id": {
                    "type": "string",
                    "description": "Asset to analyze",
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum traversal depth",
                    "default": 5,
                },
            },
            "required": ["asset_id"],
        },
    ),

    # Priority 2: Impact Analysis Tools
    Tool(
        name="analyze_impact",
        description="""Analyze the impact if an asset fails. Shows directly affected assets, cascade effects, affected process areas, and safety implications.

Example: "If PLC-101 goes down, what's affected?" -> analyze_impact(asset_id="PLC-101")""",
        inputSchema={
            "type": "object",
            "properties": {
                "asset_id": {
                    "type": "string",
                    "description": "Asset to analyze",
                },
                "failure_type": {
                    "type": "string",
                    "description": "Type of failure (complete, degraded, intermittent)",
                    "enum": ["complete", "degraded", "intermittent"],
                    "default": "complete",
                },
            },
            "required": ["asset_id"],
        },
    ),
    Tool(
        name="find_single_points_of_failure",
        description="""Identify assets that are single points of failure - assets with no redundancy where failure would cause significant impact.

Example: "What are our single points of failure?" -> find_single_points_of_failure()""",
        inputSchema={
            "type": "object",
            "properties": {
                "process_area": {
                    "type": "string",
                    "description": "Limit to specific process area",
                },
                "criticality_threshold": {
                    "type": "string",
                    "description": "Minimum criticality to consider",
                    "enum": ["critical", "high", "medium", "low"],
                    "default": "high",
                },
            },
        },
    ),

    # Priority 3: Compliance Tools
    Tool(
        name="find_gaps",
        description="""Find assets with compliance or documentation gaps (missing owner, not in CMMS, undocumented, no security policy, unverified).

Example: "What assets are missing documentation?" -> find_gaps(gap_types=["undocumented"])
Example: "What critical assets don't have an owner?" -> find_gaps(gap_types=["no_owner"], criticality="critical")""",
        inputSchema={
            "type": "object",
            "properties": {
                "gap_types": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["no_owner", "not_in_cmms", "undocumented", "no_security_policy", "unverified"],
                    },
                    "description": "Types of gaps to find (default: all)",
                },
                "process_area": {
                    "type": "string",
                    "description": "Filter to specific process area",
                },
                "criticality": {
                    "type": "string",
                    "description": "Filter by criticality",
                    "enum": ["critical", "high", "medium", "low"],
                },
            },
        },
    ),
    Tool(
        name="audit_summary",
        description="""Generate audit readiness summary with compliance statistics, gap counts, and recommendations.

Example: "What's our audit readiness?" -> audit_summary()""",
        inputSchema={
            "type": "object",
            "properties": {
                "process_area": {
                    "type": "string",
                    "description": "Filter to specific process area",
                },
                "include_recommendations": {
                    "type": "boolean",
                    "description": "Include actionable recommendations",
                    "default": True,
                },
            },
        },
    ),

    # Priority 4: Environment Tools
    Tool(
        name="list_process_areas",
        description="""List all process areas with asset counts and criticality breakdown.

Example: "What process areas do we have?" -> list_process_areas()""",
        inputSchema={
            "type": "object",
            "properties": {
                "site_id": {
                    "type": "string",
                    "description": "Filter to specific site",
                },
                "include_asset_counts": {
                    "type": "boolean",
                    "description": "Include asset counts",
                    "default": True,
                },
            },
        },
    ),
    Tool(
        name="get_process_area",
        description="""Get detailed information about a specific process area including all its assets.

Example: "Tell me about the cooling system" -> get_process_area(process_area_id="pa-cooling-system")""",
        inputSchema={
            "type": "object",
            "properties": {
                "process_area_id": {
                    "type": "string",
                    "description": "Process area ID",
                },
            },
            "required": ["process_area_id"],
        },
    ),

    # Priority 5: Review Tools
    Tool(
        name="suggest_relationship",
        description="""Suggest a new relationship between assets for human review. Use when you infer a relationship that should be verified.

Example: suggest_relationship(source_asset_id="SENS-T103", target_asset_id="PLC-101", relationship_type="feeds_data_to", reasoning="Sensor appears to provide temperature data based on naming convention")""",
        inputSchema={
            "type": "object",
            "properties": {
                "source_asset_id": {"type": "string"},
                "target_asset_id": {"type": "string"},
                "relationship_type": {
                    "type": "string",
                    "enum": ["feeds_data_to", "controls", "monitors", "safety_interlock_for", "depends_on", "redundant_with"],
                },
                "reasoning": {
                    "type": "string",
                    "description": "Explanation for why this relationship is suggested",
                },
            },
            "required": ["source_asset_id", "target_asset_id", "relationship_type", "reasoning"],
        },
    ),
    Tool(
        name="flag_for_review",
        description="""Flag an asset for human attention when you identify a potential issue.

Example: flag_for_review(asset_id="PLC-301", flag_type="ownership_unknown", description="Critical asset without assigned owner", severity="high")""",
        inputSchema={
            "type": "object",
            "properties": {
                "asset_id": {"type": "string"},
                "flag_type": {
                    "type": "string",
                    "enum": ["missing_data", "needs_verification", "potential_issue", "compliance_gap", "ownership_unknown"],
                },
                "description": {"type": "string"},
                "severity": {
                    "type": "string",
                    "enum": ["critical", "high", "medium", "low"],
                    "default": "medium",
                },
            },
            "required": ["asset_id", "flag_type", "description"],
        },
    ),
    Tool(
        name="list_review_flags",
        description="""List open review flags that need human attention.""",
        inputSchema={
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["open", "in_review", "resolved", "dismissed"],
                    "default": "open",
                },
                "severity": {
                    "type": "string",
                    "enum": ["critical", "high", "medium", "low"],
                },
            },
        },
    ),
]


@server.list_tools()
async def list_tools() -> list[Tool]:
    """Return the list of available tools."""
    return TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls from Claude."""

    try:
        result: Any = None

        # Priority 1: Core Asset Tools
        if name == "list_assets":
            result = await assets.list_assets(
                asset_type=arguments.get("asset_type"),
                process_area=arguments.get("process_area"),
                site=arguments.get("site"),
                criticality=arguments.get("criticality"),
                owner=arguments.get("owner"),
                has_gaps=arguments.get("has_gaps"),
                limit=arguments.get("limit", 50),
            )
        elif name == "get_asset":
            result = await assets.get_asset(arguments["asset_id"])
        elif name == "search_assets":
            result = await assets.search_assets(
                query=arguments["query"],
                limit=arguments.get("limit", 20),
            )

        # Priority 2: Relationship Tools
        elif name == "get_upstream":
            result = await relationships.get_upstream(
                asset_id=arguments["asset_id"],
                relationship_types=arguments.get("relationship_types"),
                max_depth=arguments.get("max_depth", 5),
            )
        elif name == "get_downstream":
            result = await relationships.get_downstream(
                asset_id=arguments["asset_id"],
                relationship_types=arguments.get("relationship_types"),
                max_depth=arguments.get("max_depth", 5),
            )
        elif name == "get_dependencies":
            result = await relationships.get_dependencies(
                asset_id=arguments["asset_id"],
                max_depth=arguments.get("max_depth", 5),
            )

        # Priority 2: Impact Analysis Tools
        elif name == "analyze_impact":
            result = await analysis.analyze_impact(
                asset_id=arguments["asset_id"],
                failure_type=arguments.get("failure_type", "complete"),
            )
        elif name == "find_single_points_of_failure":
            result = await analysis.find_single_points_of_failure(
                process_area=arguments.get("process_area"),
                criticality_threshold=arguments.get("criticality_threshold", "high"),
            )

        # Priority 3: Compliance Tools
        elif name == "find_gaps":
            result = await compliance.find_gaps(
                gap_types=arguments.get("gap_types"),
                process_area=arguments.get("process_area"),
                criticality=arguments.get("criticality"),
            )
        elif name == "audit_summary":
            result = await compliance.audit_summary(
                process_area=arguments.get("process_area"),
                include_recommendations=arguments.get("include_recommendations", True),
            )

        # Priority 4: Environment Tools
        elif name == "list_process_areas":
            result = await environment.list_process_areas(
                site_id=arguments.get("site_id"),
                include_asset_counts=arguments.get("include_asset_counts", True),
            )
        elif name == "get_process_area":
            result = await environment.get_process_area(arguments["process_area_id"])

        # Priority 5: Review Tools
        elif name == "suggest_relationship":
            result = await review.suggest_relationship(
                source_asset_id=arguments["source_asset_id"],
                target_asset_id=arguments["target_asset_id"],
                relationship_type=arguments["relationship_type"],
                reasoning=arguments["reasoning"],
            )
        elif name == "flag_for_review":
            result = await review.flag_for_review(
                asset_id=arguments["asset_id"],
                flag_type=arguments["flag_type"],
                description=arguments["description"],
                severity=arguments.get("severity", "medium"),
            )
        elif name == "list_review_flags":
            result = await review.list_review_flags(
                status=arguments.get("status", "open"),
                severity=arguments.get("severity"),
            )
        else:
            result = {"error": f"Unknown tool: {name}"}

        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


async def main():
    """Main entry point for the MCP server."""
    config = get_config()

    # Initialize database
    db_manager = DatabaseManager(config.db_path)
    await db_manager.connect()
    set_db_manager(db_manager)

    # Create tables
    await create_tables(db_manager.connection)

    # Seed sample data if configured
    if config.seed_sample_data:
        await seed_sample_data(db_manager.connection)

    # Run the server
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
