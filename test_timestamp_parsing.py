#!/usr/bin/env python3
"""Test that Airtable timestamps are being captured"""
import duckdb

# Run a sync first
print("Running sync to populate timestamps...")
# [Run your sync command here]

# Check results
conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

# Count records with timestamps
total = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
with_timestamp = conn.execute(
    """
    SELECT COUNT(*) FROM filings 
    WHERE Airtable_Last_Modified IS NOT NULL
    """
).fetchone()[0]

print(f"\nResults:")
print(f"  Total records: {total}")
print(f"  With timestamps: {with_timestamp}")
print(f"  Missing timestamps: {total - with_timestamp}")

# Show sample
print("\nSample records with timestamps:")
samples = conn.execute(
    """
    SELECT Record_ID, Company, Airtable_Last_Modified 
    FROM filings 
    WHERE Airtable_Last_Modified IS NOT NULL 
    LIMIT 3
    """
).fetchall()

for record in samples:
    print(f"  {record[0]}: {record[1]} - Modified: {record[2]}")

conn.close()
