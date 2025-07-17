#!/usr/bin/env python3
"""Add Airtable_Last_Modified column to filings table"""
import duckdb


def main():
    print("Adding Airtable_Last_Modified column...")

    try:
        conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

        # Check if column already exists
        schema = conn.execute("PRAGMA table_info(filings)").fetchall()
        columns = [col[1] for col in schema]

        if 'Airtable_Last_Modified' in columns:
            print("\u2713 Column already exists")
        else:
            # Add the column
            conn.execute(
                """
                ALTER TABLE filings 
                ADD COLUMN Airtable_Last_Modified TIMESTAMP
                """
            )
            print("\u2713 Column added successfully")

        # Verify
        schema = conn.execute("PRAGMA table_info(filings)").fetchall()
        for col in schema:
            if col[1] == 'Airtable_Last_Modified':
                print(f"\u2713 Verified: {col[1]} ({col[2]}) added to table")
                break

        conn.close()
        print("\u2705 Step 1 completed successfully!")

    except Exception as e:
        print(f"\u274c Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
