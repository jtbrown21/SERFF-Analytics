from serff_analytics.ingest.airtable_sync import AirtableSync
from serff_analytics.db import DatabaseManager


def test_connection():
    """Test database and Airtable connection"""
    print("Testing database connection...")
    db = DatabaseManager()
    with db.connection() as conn:
        # Check if table exists
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Tables in database: {tables}")

    print("\nTesting Airtable connection...")
    sync = AirtableSync()

    # Try to fetch just one record
    try:
        records = sync.table.all(max_records=1)
        print(f"Successfully connected! Found {len(records)} test record(s)")
        if records:
            print("Sample fields:", list(records[0]["fields"].keys()))
    except Exception as e:
        print(f"Airtable connection failed: {e}")


def run_sync():
    """Run the actual sync"""
    print("\nRunning full sync...")
    sync = AirtableSync()
    result = sync.sync_data()

    if result["success"]:
        print(f"\n✅ Sync successful!")
        print(f"Records processed: {result['records_processed']}")
        print(f"Total in database: {result['total_records']}")

        # Show sample data
        with DatabaseManager().connection() as conn:
            print("\nSample data:")
            sample = conn.execute(
                "SELECT Company, State, Premium_Change_Number FROM filings LIMIT 5"
            ).fetchdf()
            print(sample)
    else:
        print(f"\n❌ Sync failed: {result['error']}")


if __name__ == "__main__":
    test_connection()

    response = input("\nConnection test complete. Run full sync? (y/n): ")
    if response.lower() == "y":
        run_sync()
