"""FastAPI web server for OT Asset Inventory - Vercel deployment."""

import csv
import io
import uuid
from typing import Any

from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

import aiosqlite

# Schema
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    manufacturer TEXT,
    model TEXT,
    ip_address TEXT,
    process_area_id TEXT,
    criticality TEXT,
    owner TEXT,
    in_cmms BOOLEAN DEFAULT 0,
    documented BOOLEAN DEFAULT 0,
    security_policy_applied BOOLEAN DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS relationships (
    id TEXT PRIMARY KEY,
    source_asset_id TEXT NOT NULL,
    target_asset_id TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    verified BOOLEAN DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_assets_type ON assets(type);
CREATE INDEX IF NOT EXISTS idx_assets_criticality ON assets(criticality);
CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_asset_id);
CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_asset_id);
"""


async def get_db() -> aiosqlite.Connection:
    """Get or create database connection."""
    if not hasattr(get_db, "_db") or get_db._db is None:
        get_db._db = await aiosqlite.connect(":memory:")
        get_db._db.row_factory = aiosqlite.Row
        await get_db._db.executescript(SCHEMA_SQL)
        await get_db._db.commit()
    return get_db._db


app = FastAPI(
    title="OT Asset Inventory",
    description="AI-powered OT asset inventory for audit readiness and impact analysis",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== HTML UI ==============

@app.get("/", response_class=HTMLResponse)
async def home():
    """Serve the main UI."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OT Asset Inventory</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .loader { border-top-color: #3498db; animation: spin 1s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
    </style>
</head>
<body class="bg-gray-900 text-gray-100 min-h-screen">
    <div class="container mx-auto px-4 py-8 max-w-6xl">
        <header class="mb-8">
            <h1 class="text-3xl font-bold text-blue-400">OT Asset Inventory</h1>
            <p class="text-gray-400 mt-2">Upload assets via CSV, then query for audit readiness and impact analysis</p>
        </header>

        <!-- Upload Section -->
        <section class="bg-gray-800 rounded-lg p-6 mb-8">
            <h2 class="text-xl font-semibold mb-4">1. Upload Asset Data</h2>
            <div class="flex gap-4 items-start flex-wrap">
                <div class="flex-1 min-w-64">
                    <input type="file" id="csvFile" accept=".csv"
                        class="block w-full text-sm text-gray-400 file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:bg-blue-600 file:text-white hover:file:bg-blue-700 cursor-pointer">
                    <p class="text-xs text-gray-500 mt-2">CSV: id, name, type, manufacturer, model, ip_address, process_area, criticality, owner, in_cmms, documented, security_policy_applied</p>
                </div>
                <button onclick="uploadCSV()" class="bg-blue-600 hover:bg-blue-700 px-6 py-2 rounded font-medium">Upload</button>
                <button onclick="loadSampleData()" class="bg-green-600 hover:bg-green-700 px-6 py-2 rounded font-medium">Load Sample Data</button>
            </div>
            <div id="uploadStatus" class="mt-4 text-sm"></div>
        </section>

        <!-- Query Section -->
        <section class="bg-gray-800 rounded-lg p-6 mb-8">
            <h2 class="text-xl font-semibold mb-4">2. Query Assets</h2>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                <button onclick="runQuery('list')" class="bg-gray-700 hover:bg-gray-600 p-3 rounded text-left">
                    <div class="font-medium">List All Assets</div>
                    <div class="text-xs text-gray-400">View inventory</div>
                </button>
                <button onclick="runQuery('gaps')" class="bg-gray-700 hover:bg-gray-600 p-3 rounded text-left">
                    <div class="font-medium">Find Gaps</div>
                    <div class="text-xs text-gray-400">Compliance issues</div>
                </button>
                <button onclick="runQuery('spof')" class="bg-gray-700 hover:bg-gray-600 p-3 rounded text-left">
                    <div class="font-medium">Single Points of Failure</div>
                    <div class="text-xs text-gray-400">No redundancy</div>
                </button>
                <button onclick="runQuery('audit')" class="bg-gray-700 hover:bg-gray-600 p-3 rounded text-left">
                    <div class="font-medium">Audit Summary</div>
                    <div class="text-xs text-gray-400">Readiness report</div>
                </button>
            </div>

            <div class="flex gap-4 mt-4 flex-wrap">
                <input type="text" id="assetId" placeholder="Asset ID (e.g., PLC-101)"
                    class="flex-1 min-w-48 bg-gray-700 border border-gray-600 rounded px-4 py-2 focus:outline-none focus:border-blue-500">
                <button onclick="runQuery('impact')" class="bg-orange-600 hover:bg-orange-700 px-4 py-2 rounded font-medium">Analyze Impact</button>
                <button onclick="runQuery('upstream')" class="bg-purple-600 hover:bg-purple-700 px-4 py-2 rounded font-medium">Upstream</button>
                <button onclick="runQuery('downstream')" class="bg-teal-600 hover:bg-teal-700 px-4 py-2 rounded font-medium">Downstream</button>
            </div>
        </section>

        <!-- Results Section -->
        <section class="bg-gray-800 rounded-lg p-6">
            <h2 class="text-xl font-semibold mb-4">Results</h2>
            <div id="loading" class="hidden flex items-center gap-2 mb-4">
                <div class="loader w-5 h-5 border-2 border-gray-400 rounded-full"></div>
                <span>Loading...</span>
            </div>
            <pre id="results" class="bg-gray-900 p-4 rounded overflow-auto max-h-[500px] text-sm whitespace-pre-wrap">Click "Load Sample Data" to get started, or upload your own CSV.</pre>
        </section>
    </div>

    <script>
        async function uploadCSV() {
            const fileInput = document.getElementById('csvFile');
            const status = document.getElementById('uploadStatus');

            if (!fileInput.files[0]) {
                status.innerHTML = '<span class="text-red-400">Please select a file</span>';
                return;
            }

            const formData = new FormData();
            formData.append('file', fileInput.files[0]);

            status.innerHTML = '<span class="text-blue-400">Uploading...</span>';

            try {
                const response = await fetch('/api/upload-csv', { method: 'POST', body: formData });
                const data = await response.json();
                if (response.ok) {
                    status.innerHTML = '<span class="text-green-400">✓ Uploaded ' + data.assets_imported + ' assets</span>';
                    runQuery('list');
                } else {
                    status.innerHTML = '<span class="text-red-400">Error: ' + data.detail + '</span>';
                }
            } catch (err) {
                status.innerHTML = '<span class="text-red-400">Error: ' + err.message + '</span>';
            }
        }

        async function loadSampleData() {
            const status = document.getElementById('uploadStatus');
            status.innerHTML = '<span class="text-blue-400">Loading sample data...</span>';

            try {
                const response = await fetch('/api/load-sample-data', { method: 'POST' });
                const data = await response.json();
                if (response.ok) {
                    status.innerHTML = '<span class="text-green-400">✓ Loaded ' + data.assets_count + ' assets, ' + data.relationships_count + ' relationships</span>';
                    runQuery('list');
                } else {
                    status.innerHTML = '<span class="text-red-400">Error: ' + data.detail + '</span>';
                }
            } catch (err) {
                status.innerHTML = '<span class="text-red-400">Error: ' + err.message + '</span>';
            }
        }

        async function runQuery(type) {
            const loading = document.getElementById('loading');
            const results = document.getElementById('results');
            const assetId = document.getElementById('assetId').value || 'PLC-101';

            loading.classList.remove('hidden');
            results.textContent = '';

            const urls = {
                'list': '/api/assets',
                'gaps': '/api/gaps',
                'spof': '/api/spof',
                'audit': '/api/audit',
                'impact': '/api/impact/' + assetId,
                'upstream': '/api/upstream/' + assetId,
                'downstream': '/api/downstream/' + assetId,
            };

            try {
                const response = await fetch(urls[type]);
                const data = await response.json();
                results.textContent = JSON.stringify(data, null, 2);
            } catch (err) {
                results.textContent = 'Error: ' + err.message;
            } finally {
                loading.classList.add('hidden');
            }
        }
    </script>
</body>
</html>
"""


# ============== API Endpoints ==============

@app.post("/api/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """Upload assets from CSV file."""
    db = await get_db()

    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "File must be a CSV")

    content = await file.read()
    text = content.decode('utf-8')
    reader = csv.DictReader(io.StringIO(text))

    count = 0
    for row in reader:
        asset_id = row.get('id') or row.get('asset_id') or f"ASSET-{uuid.uuid4().hex[:8].upper()}"
        await db.execute("""
            INSERT OR REPLACE INTO assets (id, name, type, manufacturer, model, ip_address, process_area_id, criticality, owner, in_cmms, documented, security_policy_applied, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            asset_id,
            row.get('name', 'Unknown'),
            row.get('type', 'Unknown'),
            row.get('manufacturer'),
            row.get('model'),
            row.get('ip_address'),
            row.get('process_area') or row.get('process_area_id'),
            row.get('criticality', 'medium'),
            row.get('owner'),
            1 if row.get('in_cmms', '').lower() in ('true', 'yes', '1') else 0,
            1 if row.get('documented', '').lower() in ('true', 'yes', '1') else 0,
            1 if row.get('security_policy_applied', '').lower() in ('true', 'yes', '1') else 0,
            row.get('notes'),
        ))
        count += 1

    await db.commit()
    return {"status": "success", "assets_imported": count}


@app.post("/api/load-sample-data")
async def load_sample_data():
    """Load sample manufacturing data."""
    db = await get_db()

    sample_assets = [
        ("PLC-101", "Main Chiller Controller", "PLC", "Allen-Bradley", "ControlLogix 5580", "192.168.10.101", "pa-cooling", "critical", "John Smith", 1, 1, 1),
        ("PLC-201", "Packaging Line Controller", "PLC", "Siemens", "S7-1500", "192.168.20.101", "pa-packaging", "critical", "Mike Johnson", 1, 1, 1),
        ("PLC-301", "Compressed Air Controller", "PLC", "Allen-Bradley", "CompactLogix 5380", "192.168.30.101", "pa-utilities", "critical", None, 1, 1, 0),
        ("HMI-101", "Cooling System HMI", "HMI", "Rockwell", "PanelView Plus 7", "192.168.10.111", "pa-cooling", "high", "John Smith", 1, 1, 0),
        ("HMI-201", "Packaging Line HMI", "HMI", "Siemens", "Comfort Panel", "192.168.20.111", "pa-packaging", "medium", "Mike Johnson", 1, 0, 0),
        ("SENS-T101", "Chiller Supply Temp Sensor", "Sensor", "Emerson", "Rosemount 3144P", "192.168.10.201", "pa-cooling", "high", None, 0, 0, 0),
        ("SENS-T102", "Chiller Return Temp Sensor", "Sensor", "Emerson", "Rosemount 3144P", "192.168.10.202", "pa-cooling", "high", None, 1, 0, 0),
        ("SENS-P101", "Cooling Water Pressure", "Sensor", "Endress+Hauser", "Cerabar PMC51", "192.168.10.203", "pa-cooling", "medium", "Jane Doe", 1, 1, 1),
        ("ACT-V101", "Cooling Water Control Valve", "Actuator", "Fisher", "DVC6200", "192.168.10.151", "pa-cooling", "high", "Jane Doe", 1, 1, 1),
        ("ACT-M201", "Conveyor Motor Drive", "Actuator", "ABB", "ACS580", "192.168.20.201", "pa-packaging", "high", "Mike Johnson", 1, 1, 1),
        ("SENS-PE201", "Carton Presence Sensor", "Sensor", "SICK", "WTB4S-3", None, "pa-packaging", "medium", None, 0, 0, 0),
        ("ACT-C301", "Air Compressor 1", "Actuator", "Atlas Copco", "GA 90+", "192.168.30.151", "pa-utilities", "critical", "Sarah Wilson", 1, 1, 1),
        ("ACT-C302", "Air Compressor 2 (Backup)", "Actuator", "Atlas Copco", "GA 90+", "192.168.30.152", "pa-utilities", "high", "Sarah Wilson", 1, 1, 1),
        ("GW-001", "Plant Network Gateway", "Gateway", "Cisco", "IE-4000", "192.168.1.1", None, "critical", "IT Dept", 0, 1, 1),
        ("SW-010", "Cooling VLAN Switch", "Switch", "Cisco", "IE-3400", "192.168.10.1", "pa-cooling", "high", "IT Dept", 0, 1, 1),
    ]

    for asset in sample_assets:
        await db.execute("""
            INSERT OR REPLACE INTO assets (id, name, type, manufacturer, model, ip_address, process_area_id, criticality, owner, in_cmms, documented, security_policy_applied)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, asset)

    relationships = [
        ("SENS-T101", "PLC-101", "feeds_data_to"),
        ("SENS-T102", "PLC-101", "feeds_data_to"),
        ("SENS-P101", "PLC-101", "feeds_data_to"),
        ("PLC-101", "ACT-V101", "controls"),
        ("PLC-101", "HMI-101", "feeds_data_to"),
        ("PLC-201", "PLC-101", "depends_on"),
        ("PLC-201", "ACT-M201", "controls"),
        ("PLC-201", "HMI-201", "feeds_data_to"),
        ("SENS-PE201", "PLC-201", "feeds_data_to"),
        ("PLC-201", "PLC-301", "depends_on"),
        ("PLC-301", "ACT-C301", "controls"),
        ("PLC-301", "ACT-C302", "controls"),
        ("ACT-C301", "ACT-C302", "redundant_with"),
        ("ACT-C301", "PLC-101", "depends_on"),
    ]

    for src, tgt, rel_type in relationships:
        await db.execute("""
            INSERT OR REPLACE INTO relationships (id, source_asset_id, target_asset_id, relationship_type, verified)
            VALUES (?, ?, ?, ?, 1)
        """, (f"rel-{src}-{tgt}", src, tgt, rel_type))

    await db.commit()
    return {"status": "success", "assets_count": len(sample_assets), "relationships_count": len(relationships)}


@app.get("/api/assets")
async def list_assets(type: str = None, criticality: str = None, has_gaps: bool = False):
    """List assets."""
    db = await get_db()
    query = "SELECT * FROM assets WHERE 1=1"
    params = []

    if type:
        query += " AND type = ?"
        params.append(type)
    if criticality:
        query += " AND criticality = ?"
        params.append(criticality)
    if has_gaps:
        query += " AND (owner IS NULL OR NOT in_cmms OR NOT documented OR NOT security_policy_applied)"

    query += " ORDER BY criticality DESC, name"

    async with db.execute(query, params) as cursor:
        return [dict(row) for row in await cursor.fetchall()]


@app.get("/api/assets/{asset_id}")
async def get_asset(asset_id: str):
    """Get asset details."""
    db = await get_db()
    async with db.execute("SELECT * FROM assets WHERE id = ?", [asset_id]) as cursor:
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(404, f"Asset {asset_id} not found")
        asset = dict(row)

    async with db.execute("SELECT * FROM relationships WHERE source_asset_id = ?", [asset_id]) as cursor:
        asset["outgoing"] = [dict(r) for r in await cursor.fetchall()]

    async with db.execute("SELECT * FROM relationships WHERE target_asset_id = ?", [asset_id]) as cursor:
        asset["incoming"] = [dict(r) for r in await cursor.fetchall()]

    return asset


@app.get("/api/gaps")
async def find_gaps():
    """Find compliance gaps."""
    db = await get_db()
    gaps = {}

    async with db.execute("SELECT id, name, type, criticality FROM assets WHERE owner IS NULL ORDER BY criticality DESC") as cursor:
        gaps["no_owner"] = [dict(r) for r in await cursor.fetchall()]

    async with db.execute("SELECT id, name, type, criticality FROM assets WHERE NOT in_cmms ORDER BY criticality DESC") as cursor:
        gaps["not_in_cmms"] = [dict(r) for r in await cursor.fetchall()]

    async with db.execute("SELECT id, name, type, criticality FROM assets WHERE NOT documented ORDER BY criticality DESC") as cursor:
        gaps["undocumented"] = [dict(r) for r in await cursor.fetchall()]

    async with db.execute("SELECT id, name, type, criticality FROM assets WHERE NOT security_policy_applied ORDER BY criticality DESC") as cursor:
        gaps["no_security_policy"] = [dict(r) for r in await cursor.fetchall()]

    return {"gaps": gaps, "summary": {k: len(v) for k, v in gaps.items()}}


@app.get("/api/spof")
async def find_spof():
    """Find single points of failure."""
    db = await get_db()
    spofs = []

    async with db.execute("SELECT * FROM assets WHERE criticality IN ('critical', 'high')") as cursor:
        assets = await cursor.fetchall()

    for asset in assets:
        asset_id = asset["id"]

        async with db.execute("""
            SELECT COUNT(*) as cnt FROM relationships
            WHERE (source_asset_id = ? OR target_asset_id = ?) AND relationship_type = 'redundant_with'
        """, [asset_id, asset_id]) as cursor:
            if (await cursor.fetchone())["cnt"] > 0:
                continue

        async with db.execute("SELECT COUNT(*) as cnt FROM relationships WHERE target_asset_id = ? AND relationship_type = 'depends_on'", [asset_id]) as cursor:
            dependent_count = (await cursor.fetchone())["cnt"]

        async with db.execute("SELECT COUNT(*) as cnt FROM relationships WHERE source_asset_id = ?", [asset_id]) as cursor:
            downstream_count = (await cursor.fetchone())["cnt"]

        if dependent_count > 0 or downstream_count > 2:
            spofs.append({
                "id": asset["id"], "name": asset["name"], "type": asset["type"],
                "criticality": asset["criticality"], "dependent_count": dependent_count,
                "downstream_count": downstream_count,
                "risk": "HIGH" if asset["criticality"] == "critical" else "MEDIUM",
            })

    return sorted(spofs, key=lambda x: (x["criticality"] != "critical", -x["dependent_count"]))


@app.get("/api/audit")
async def audit_summary():
    """Audit readiness summary."""
    db = await get_db()

    async with db.execute("SELECT COUNT(*) as total FROM assets") as cursor:
        total = (await cursor.fetchone())["total"]

    if total == 0:
        return {"error": "No assets. Load sample data or upload CSV."}

    async with db.execute("""
        SELECT SUM(CASE WHEN owner IS NOT NULL THEN 1 ELSE 0 END) as has_owner,
               SUM(CASE WHEN in_cmms THEN 1 ELSE 0 END) as in_cmms,
               SUM(CASE WHEN documented THEN 1 ELSE 0 END) as documented,
               SUM(CASE WHEN security_policy_applied THEN 1 ELSE 0 END) as has_security
        FROM assets
    """) as cursor:
        stats = await cursor.fetchone()

    async with db.execute("SELECT type, COUNT(*) as count FROM assets GROUP BY type") as cursor:
        by_type = {r["type"]: r["count"] for r in await cursor.fetchall()}

    async with db.execute("SELECT criticality, COUNT(*) as count FROM assets GROUP BY criticality") as cursor:
        by_crit = {r["criticality"] or "unassigned": r["count"] for r in await cursor.fetchall()}

    def pct(n): return round((n or 0) / total * 100, 1)

    score = (pct(stats["has_owner"]) * 0.25 + pct(stats["in_cmms"]) * 0.20 +
             pct(stats["documented"]) * 0.25 + pct(stats["has_security"]) * 0.30)

    return {
        "total_assets": total, "by_type": by_type, "by_criticality": by_crit,
        "compliance": {
            "has_owner": {"count": stats["has_owner"], "pct": pct(stats["has_owner"])},
            "in_cmms": {"count": stats["in_cmms"], "pct": pct(stats["in_cmms"])},
            "documented": {"count": stats["documented"], "pct": pct(stats["documented"])},
            "security_policy": {"count": stats["has_security"], "pct": pct(stats["has_security"])},
        },
        "score": round(score, 1),
        "grade": "A" if score >= 90 else "B" if score >= 80 else "C" if score >= 70 else "D" if score >= 60 else "F",
    }


@app.get("/api/impact/{asset_id}")
async def analyze_impact(asset_id: str):
    """Analyze failure impact."""
    db = await get_db()

    async with db.execute("SELECT * FROM assets WHERE id = ?", [asset_id]) as cursor:
        asset = await cursor.fetchone()
        if not asset:
            raise HTTPException(404, f"Asset {asset_id} not found")

    async with db.execute("""
        SELECT a.id, a.name, a.type, a.criticality, r.relationship_type
        FROM relationships r JOIN assets a ON r.target_asset_id = a.id WHERE r.source_asset_id = ?
    """, [asset_id]) as cursor:
        directly_affected = [dict(r) for r in await cursor.fetchall()]

    async with db.execute("""
        SELECT a.id, a.name, a.type, a.criticality
        FROM relationships r JOIN assets a ON r.source_asset_id = a.id
        WHERE r.target_asset_id = ? AND r.relationship_type = 'depends_on'
    """, [asset_id]) as cursor:
        cascade = [dict(r) for r in await cursor.fetchall()]

    async with db.execute("""
        SELECT COUNT(*) as cnt FROM relationships
        WHERE (source_asset_id = ? OR target_asset_id = ?) AND relationship_type = 'redundant_with'
    """, [asset_id, asset_id]) as cursor:
        has_redundancy = (await cursor.fetchone())["cnt"] > 0

    all_affected = directly_affected + cascade
    crit_count = len([a for a in all_affected if a.get("criticality") == "critical"])

    return {
        "asset": {"id": asset["id"], "name": asset["name"], "type": asset["type"], "criticality": asset["criticality"]},
        "directly_affected": directly_affected,
        "cascade_effects": cascade,
        "total_affected": len(all_affected),
        "critical_affected": crit_count,
        "has_redundancy": has_redundancy,
        "risk_level": "CRITICAL" if not has_redundancy and crit_count > 0 else "HIGH" if not has_redundancy else "MEDIUM",
    }


@app.get("/api/upstream/{asset_id}")
async def get_upstream(asset_id: str):
    """Get upstream assets."""
    db = await get_db()
    visited, result, queue = set(), [], [(asset_id, 0)]

    while queue:
        current_id, depth = queue.pop(0)
        if current_id in visited or depth > 5:
            continue
        visited.add(current_id)

        async with db.execute("""
            SELECT a.id, a.name, a.type, a.criticality, r.relationship_type
            FROM relationships r JOIN assets a ON r.source_asset_id = a.id WHERE r.target_asset_id = ?
        """, [current_id]) as cursor:
            for row in await cursor.fetchall():
                if row["id"] not in visited:
                    result.append({**dict(row), "depth": depth + 1})
                    queue.append((row["id"], depth + 1))

    return {"asset_id": asset_id, "upstream": result, "count": len(result)}


@app.get("/api/downstream/{asset_id}")
async def get_downstream(asset_id: str):
    """Get downstream assets."""
    db = await get_db()
    visited, result, queue = set(), [], [(asset_id, 0)]

    while queue:
        current_id, depth = queue.pop(0)
        if current_id in visited or depth > 5:
            continue
        visited.add(current_id)

        async with db.execute("""
            SELECT a.id, a.name, a.type, a.criticality, r.relationship_type
            FROM relationships r JOIN assets a ON r.target_asset_id = a.id WHERE r.source_asset_id = ?
        """, [current_id]) as cursor:
            for row in await cursor.fetchall():
                if row["id"] not in visited:
                    result.append({**dict(row), "depth": depth + 1})
                    queue.append((row["id"], depth + 1))

    return {"asset_id": asset_id, "downstream": result, "count": len(result)}
