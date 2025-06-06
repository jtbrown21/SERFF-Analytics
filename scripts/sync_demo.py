import json
from datetime import datetime
from pathlib import Path
import argparse

from serff_analytics.ingest.airtable_sync import AirtableSync
from serff_analytics.db import DatabaseManager


SYNC_FILE = Path(".last_sync.json")


def get_last_sync_time() -> str | None:
    """Return the ISO timestamp of the last successful sync if it exists."""
    if SYNC_FILE.exists():
        with SYNC_FILE.open("r") as f:
            data = json.load(f)
            return data.get("last_sync")
    return None


def save_sync_time(timestamp: str) -> None:
    """Persist the timestamp of the latest sync."""
    with SYNC_FILE.open("w") as f:
        json.dump({"last_sync": timestamp}, f)


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


def run_sync(since: datetime | None = None) -> dict:
    """Run the sync and return the result dictionary."""
    sync_type = "incremental" if since else "full"
    print(f"\nRunning {sync_type} sync...")

    sync = AirtableSync()
    result = sync.sync_data(since=since)

    if result["success"]:
        print("\n✅ Sync successful!")
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

    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full",
        action="store_true",
        help="Force full sync (ignore saved timestamp)",
    )
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Only test database and Airtable connections",
    )
    args = parser.parse_args()

    if args.test_connection:
        test_connection()
        return

    last_sync_str = None if args.full else get_last_sync_time()
    since = datetime.fromisoformat(last_sync_str) if last_sync_str else None

    if since:
        print(f"🔄 Incremental sync since {last_sync_str}")
    else:
        print("🔄 Full sync")

    result = run_sync(since)

    if result.get("success"):
        save_sync_time(datetime.utcnow().isoformat())
        print("✅ Sync timestamp saved")


if __name__ == "__main__":
    main()
