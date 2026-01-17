"""Basic tests for OT Asset Inventory."""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ot_asset_inventory.db.connection import DatabaseManager, set_db_manager
from ot_asset_inventory.db.schema import create_tables
from ot_asset_inventory.db.seed import seed_sample_data
from ot_asset_inventory.tools import assets, relationships, analysis, compliance, environment


async def setup_test_db():
    """Set up an in-memory test database."""
    db_manager = DatabaseManager(":memory:")
    await db_manager.connect()
    set_db_manager(db_manager)
    await create_tables(db_manager.connection)

    # Seed from sample data file
    sample_data_path = Path(__file__).parent.parent / "data" / "sample_data.json"
    await seed_sample_data(db_manager.connection, sample_data_path)

    return db_manager


async def test_list_assets():
    """Test listing assets."""
    print("\n=== Test: list_assets ===")

    # List all assets
    all_assets = await assets.list_assets()
    print(f"Total assets: {len(all_assets)}")
    assert len(all_assets) > 0, "Should have assets"

    # Filter by type
    plcs = await assets.list_assets(asset_type="PLC")
    print(f"PLCs: {len(plcs)}")
    for plc in plcs:
        print(f"  - {plc['id']}: {plc['name']}")
    assert all(a["type"] == "PLC" for a in plcs), "All should be PLCs"

    # Filter by process area
    cooling_assets = await assets.list_assets(process_area="Cooling")
    print(f"Cooling system assets: {len(cooling_assets)}")
    assert len(cooling_assets) > 0, "Should have cooling assets"

    print("✓ list_assets tests passed")


async def test_get_asset():
    """Test getting asset details."""
    print("\n=== Test: get_asset ===")

    asset = await assets.get_asset("PLC-101")
    assert asset is not None, "Should find PLC-101"
    print(f"Asset: {asset['name']}")
    print(f"Type: {asset['type']}")
    print(f"Criticality: {asset['criticality']}")
    print(f"Outgoing relationships: {len(asset.get('outgoing_relationships', []))}")
    print(f"Incoming relationships: {len(asset.get('incoming_relationships', []))}")

    # Check compliance summary
    comp = asset.get("compliance_summary", {})
    print(f"Compliance gaps: {comp.get('gap_count', 'N/A')}")

    print("✓ get_asset tests passed")


async def test_search_assets():
    """Test searching assets."""
    print("\n=== Test: search_assets ===")

    results = await assets.search_assets("chiller")
    print(f"Search 'chiller': {len(results)} results")
    for r in results:
        print(f"  - {r['id']}: {r['name']}")
    assert len(results) > 0, "Should find chiller-related assets"

    print("✓ search_assets tests passed")


async def test_upstream_downstream():
    """Test upstream/downstream queries."""
    print("\n=== Test: upstream/downstream ===")

    # Get upstream of PLC-101 (sensors that feed into it)
    upstream = await relationships.get_upstream("PLC-101")
    print(f"Upstream of PLC-101: {len(upstream['assets'])} assets")
    for a in upstream["assets"]:
        print(f"  - {a['id']}: {a['name']} (depth {a['depth']})")

    # Get downstream of PLC-101 (what it controls)
    downstream = await relationships.get_downstream("PLC-101")
    print(f"Downstream of PLC-101: {len(downstream['assets'])} assets")
    for a in downstream["assets"]:
        print(f"  - {a['id']}: {a['name']} (depth {a['depth']})")

    print("✓ upstream/downstream tests passed")


async def test_analyze_impact():
    """Test impact analysis."""
    print("\n=== Test: analyze_impact ===")

    impact = await analysis.analyze_impact("PLC-101")
    print(f"Impact analysis for {impact['failing_asset']['name']}:")
    print(f"  Directly affected: {impact['directly_affected_count']}")
    print(f"  Cascade effects: {impact['cascade_count']}")
    print(f"  Total affected: {impact['total_affected']}")
    print(f"  Affected process areas: {impact['affected_process_areas']}")
    print(f"  Safety implications: {impact['safety_implications']}")
    print(f"  Has redundancy: {impact['has_redundancy']}")

    if impact["recommendations"]:
        print("  Recommendations:")
        for rec in impact["recommendations"]:
            print(f"    - {rec}")

    print("✓ analyze_impact tests passed")


async def test_find_spof():
    """Test single point of failure detection."""
    print("\n=== Test: find_single_points_of_failure ===")

    spofs = await analysis.find_single_points_of_failure()
    print(f"Found {len(spofs)} single points of failure:")
    for spof in spofs[:5]:  # Show top 5
        print(f"  - {spof['id']}: {spof['name']}")
        print(f"    Risk: {spof['risk_level']} (score: {spof['risk_score']})")
        print(f"    Dependents: {spof['dependent_count']}, Downstream: {spof['downstream_count']}")
        print(f"    Recommendation: {spof['recommendation']}")

    # PLC-101 should be identified as SPOF
    spof_ids = [s["id"] for s in spofs]
    assert "PLC-101" in spof_ids, "PLC-101 should be identified as SPOF"

    print("✓ find_single_points_of_failure tests passed")


async def test_find_gaps():
    """Test finding compliance gaps."""
    print("\n=== Test: find_gaps ===")

    gaps = await compliance.find_gaps()
    print("Gap summary:")
    for gap_type, count in gaps["summary"]["gap_counts"].items():
        print(f"  {gap_type}: {count}")

    # Check for assets without owners
    no_owner = gaps["gaps"].get("no_owner", [])
    print(f"\nAssets without owner ({len(no_owner)}):")
    for asset in no_owner[:5]:
        print(f"  - {asset['id']}: {asset['name']} ({asset['criticality']})")

    print("✓ find_gaps tests passed")


async def test_audit_summary():
    """Test audit summary."""
    print("\n=== Test: audit_summary ===")

    summary = await compliance.audit_summary()
    print(f"Audit Summary (as of {summary['audit_date']}):")
    print(f"  Total assets: {summary['total_assets']}")
    print(f"  Compliance score: {summary['overall_compliance_score']['score']} ({summary['overall_compliance_score']['grade']})")

    print("\n  Compliance statistics:")
    for key, val in summary["compliance_statistics"].items():
        print(f"    {key}: {val['count']} ({val['percentage']}%)")

    if summary.get("recommendations"):
        print("\n  Recommendations:")
        for rec in summary["recommendations"][:3]:
            print(f"    - {rec}")

    print("✓ audit_summary tests passed")


async def test_process_areas():
    """Test process area queries."""
    print("\n=== Test: list_process_areas ===")

    process_areas = await environment.list_process_areas()
    print(f"Process areas: {len(process_areas)}")
    for pa in process_areas:
        print(f"  - {pa['name']}: {pa.get('asset_count', 'N/A')} assets")

    print("✓ list_process_areas tests passed")


async def main():
    """Run all tests."""
    print("=" * 60)
    print("OT Asset Inventory - Basic Tests")
    print("=" * 60)

    # Setup
    db_manager = await setup_test_db()

    try:
        # Run tests
        await test_list_assets()
        await test_get_asset()
        await test_search_assets()
        await test_upstream_downstream()
        await test_analyze_impact()
        await test_find_spof()
        await test_find_gaps()
        await test_audit_summary()
        await test_process_areas()

        print("\n" + "=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)

    finally:
        await db_manager.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
