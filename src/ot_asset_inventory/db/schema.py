"""SQLite database schema definitions for OT Asset Inventory."""

import aiosqlite


SCHEMA_SQL = """
-- Environments table (top-level organizational unit)
CREATE TABLE IF NOT EXISTS environments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('manufacturing', 'water_treatment', 'energy', 'chemical', 'food_beverage', 'pharmaceutical')),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sites within environments
CREATE TABLE IF NOT EXISTS sites (
    id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL REFERENCES environments(id),
    name TEXT NOT NULL,
    address TEXT,
    timezone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Process areas within sites
CREATE TABLE IF NOT EXISTS process_areas (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(id),
    name TEXT NOT NULL,
    description TEXT,
    function TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Compliance frameworks linked to environments
CREATE TABLE IF NOT EXISTS compliance_frameworks (
    id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL REFERENCES environments(id),
    name TEXT NOT NULL,
    version TEXT,
    description TEXT
);

-- Main assets table
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('PLC', 'HMI', 'Sensor', 'Actuator', 'RTU', 'Gateway', 'Switch', 'Server', 'Workstation')),
    manufacturer TEXT,
    model TEXT,
    serial_number TEXT,
    firmware_version TEXT,

    -- Location
    site_id TEXT REFERENCES sites(id),
    building TEXT,
    area TEXT,
    zone TEXT,
    process_area_id TEXT REFERENCES process_areas(id),

    -- Network information
    ip_address TEXT,
    mac_address TEXT,
    vlan INTEGER,
    protocols TEXT,  -- JSON array

    -- Environment context
    environment_type TEXT,
    function TEXT,

    -- Ownership
    owner TEXT,
    maintainer TEXT,
    last_verified DATE,

    -- Compliance status
    in_cmms BOOLEAN DEFAULT 0,
    documented BOOLEAN DEFAULT 0,
    security_policy_applied BOOLEAN DEFAULT 0,

    -- Risk assessment
    criticality TEXT CHECK (criticality IN ('critical', 'high', 'medium', 'low')),

    -- Metadata
    notes TEXT,
    tags TEXT,  -- JSON array
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Relationships between assets
CREATE TABLE IF NOT EXISTS relationships (
    id TEXT PRIMARY KEY,
    source_asset_id TEXT NOT NULL REFERENCES assets(id),
    target_asset_id TEXT NOT NULL REFERENCES assets(id),
    relationship_type TEXT NOT NULL CHECK (relationship_type IN (
        'feeds_data_to', 'controls', 'monitors',
        'safety_interlock_for', 'depends_on', 'redundant_with',
        'communicates_with', 'powers', 'backs_up'
    )),
    inferred BOOLEAN DEFAULT 0,
    verified BOOLEAN DEFAULT 0,
    verified_by TEXT,
    verified_at TIMESTAMP,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Review flags for human validation
CREATE TABLE IF NOT EXISTS review_flags (
    id TEXT PRIMARY KEY,
    asset_id TEXT REFERENCES assets(id),
    relationship_id TEXT REFERENCES relationships(id),
    flag_type TEXT NOT NULL CHECK (flag_type IN (
        'missing_data', 'needs_verification', 'potential_issue',
        'suggested_relationship', 'compliance_gap', 'ownership_unknown'
    )),
    description TEXT NOT NULL,
    severity TEXT CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    status TEXT DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'resolved', 'dismissed')),
    flagged_by TEXT DEFAULT 'system',
    flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_by TEXT,
    resolved_at TIMESTAMP,
    resolution_notes TEXT
);

-- Audit log for tracking changes
CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    changes TEXT,  -- JSON diff
    performed_by TEXT,
    performed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INDEX_SQL = """
-- Asset queries
CREATE INDEX IF NOT EXISTS idx_assets_type ON assets(type);
CREATE INDEX IF NOT EXISTS idx_assets_process_area ON assets(process_area_id);
CREATE INDEX IF NOT EXISTS idx_assets_site ON assets(site_id);
CREATE INDEX IF NOT EXISTS idx_assets_criticality ON assets(criticality);
CREATE INDEX IF NOT EXISTS idx_assets_owner ON assets(owner);

-- Relationship queries (critical for graph traversal)
CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_asset_id);
CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_asset_id);
CREATE INDEX IF NOT EXISTS idx_relationships_type ON relationships(relationship_type);

-- Review flags
CREATE INDEX IF NOT EXISTS idx_review_flags_status ON review_flags(status);
CREATE INDEX IF NOT EXISTS idx_review_flags_asset ON review_flags(asset_id);

-- Process areas
CREATE INDEX IF NOT EXISTS idx_process_areas_site ON process_areas(site_id);

-- Sites
CREATE INDEX IF NOT EXISTS idx_sites_environment ON sites(environment_id);
"""


async def create_tables(db: aiosqlite.Connection) -> None:
    """Create all database tables."""
    await db.executescript(SCHEMA_SQL)
    await db.executescript(INDEX_SQL)
    await db.commit()


async def drop_tables(db: aiosqlite.Connection) -> None:
    """Drop all database tables (use with caution)."""
    tables = [
        "audit_log", "review_flags", "relationships", "assets",
        "compliance_frameworks", "process_areas", "sites", "environments"
    ]
    for table in tables:
        await db.execute(f"DROP TABLE IF EXISTS {table}")
    await db.commit()
