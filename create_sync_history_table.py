#!/usr/bin/env python3
"""Create sync_history table for tracking sync operations"""
import duckdb


def main():
    print("Creating sync_history table...")

    try:
        conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

        # Create the table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_history (
                sync_id INTEGER PRIMARY KEY,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                records_processed INTEGER DEFAULT 0,
                status VARCHAR(50) DEFAULT 'running'
            )
            """
        )

        # Create sequence for auto-increment
        conn.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS sync_history_seq START 1
            """
        )

        print("\u2713 Table created successfully")

        # Verify
        tables = conn.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name = 'sync_history'
            """
        ).fetchall()

        if tables:
            print("\u2713 Verified: sync_history table exists")

        conn.close()
        print("\u2705 Step 4 completed successfully!")

    except Exception as e:
        print(f"\u274c Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())

