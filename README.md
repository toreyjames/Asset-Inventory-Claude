# OT Asset Inventory MCP Server

An MCP (Model Context Protocol) server that exposes an OT (Operational Technology) asset inventory to Claude, enabling natural language queries for audit readiness, gap analysis, and impact/risk assessment.

## Features

- **Asset Queries**: List, search, and filter assets by type, location, criticality, and compliance status
- **Relationship Tracking**: Map upstream/downstream dependencies between assets
- **Impact Analysis**: Analyze cascade effects if an asset fails
- **Single Point of Failure Detection**: Identify critical assets without redundancy
- **Compliance Gap Analysis**: Find assets missing owners, documentation, CMMS entries, or security policies
- **Audit Readiness**: Generate compliance summaries and recommendations
- **Human-in-the-Loop**: Flag assets for review and suggest relationships for verification

## Installation

1. Clone this repository:
```bash
cd "/Users/toreyhall/Documents/Asset Inventory Claude Code"
```

2. Create a virtual environment and install dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install mcp aiosqlite pydantic
```

3. Run the tests to verify everything works:
```bash
python tests/test_basic.py
```

## Claude Desktop Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ot-asset-inventory": {
      "command": "/Users/toreyhall/Documents/Asset Inventory Claude Code/.venv/bin/python",
      "args": [
        "-m",
        "src.ot_asset_inventory.server"
      ],
      "cwd": "/Users/toreyhall/Documents/Asset Inventory Claude Code"
    }
  }
}
```

After adding the configuration, restart Claude Desktop.

## Available Tools

### Core Asset Tools (Priority 1)
- **list_assets**: List/filter assets by type, location, criticality, compliance status
- **get_asset**: Get full details for a specific asset including relationships
- **search_assets**: Natural language search across all fields

### Relationship Tools (Priority 2)
- **get_upstream**: Find assets that feed data into this asset
- **get_downstream**: Find assets this asset feeds/controls
- **get_dependencies**: Complete dependency map (both directions)

### Impact Analysis Tools (Priority 2)
- **analyze_impact**: If this asset goes down, what's affected?
- **find_single_points_of_failure**: Assets with no redundancy supporting critical functions

### Compliance Tools (Priority 3)
- **find_gaps**: Find assets missing documentation, owner, CMMS entry, verification, security policy
- **audit_summary**: Generate audit-ready summary with coverage percentages and recommendations

### Environment Tools (Priority 4)
- **list_process_areas**: List process areas with asset counts
- **get_process_area**: Get process area details with all assets

### Review Tools (Priority 5)
- **suggest_relationship**: AI suggests a relationship for human validation
- **flag_for_review**: Flag something for human attention
- **list_review_flags**: View open flags needing attention

## Example Queries

Once configured, ask Claude:

- "What PLCs do we have in the cooling system?"
- "If PLC-101 goes down, what's affected?"
- "What critical assets don't have an owner?"
- "Show me everything upstream of the main chiller controller"
- "What's our audit readiness - how many assets are verified and documented?"
- "Find our single points of failure"

## Sample Data

The server includes sample data representing a small manufacturing environment:
- 24 assets across 3 process areas (Cooling System, Packaging Line, Utilities)
- Mix of PLCs, HMIs, sensors, actuators, network equipment
- Intentional gaps for testing (assets without owners, not in CMMS, etc.)
- Relationship graph showing dependencies
- PLC-101 configured as a single point of failure scenario

## Project Structure

```
├── src/
│   └── ot_asset_inventory/
│       ├── server.py          # MCP server entry point
│       ├── config.py          # Configuration management
│       ├── db/
│       │   ├── schema.py      # SQLite schema
│       │   ├── connection.py  # Database connection
│       │   └── seed.py        # Sample data seeding
│       ├── models/            # Data models
│       ├── tools/             # MCP tool implementations
│       │   ├── assets.py      # Asset queries
│       │   ├── relationships.py # Relationship queries
│       │   ├── analysis.py    # Impact analysis
│       │   ├── compliance.py  # Gap analysis, audits
│       │   ├── environment.py # Environment/process areas
│       │   └── review.py      # Human-in-the-loop
│       └── utils/
│           └── graph.py       # Graph traversal algorithms
├── data/
│   └── sample_data.json       # Sample manufacturing environment
└── tests/
    └── test_basic.py          # Basic functionality tests
```

## Environment Variables

- `OT_INVENTORY_DB_PATH`: Path to SQLite database (default: `data/inventory.db`)
- `OT_INVENTORY_LOG_LEVEL`: Log level (default: `INFO`)
- `OT_INVENTORY_SEED_DATA`: Whether to seed sample data (default: `true`)

## License

MIT
