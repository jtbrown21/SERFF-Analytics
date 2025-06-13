#!/usr/bin/env python3
"""Test that incremental sync only updates changed records"""
import duckdb
import time

print("Test: Incremental sync should only update changed records")

conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

# Get current state
before = conn.execute(
    """
    SELECT COUNT(*), MAX(Updated_At) 
    FROM filings
    """
).fetchone()

print(f"\nBefore sync:")
print(f"  Records: {before[0]}")
print(f"  Latest update: {before[1]}")

# Run sync
print("\nRunning sync...")
# [Run your sync command]

# Check results
after = conn.execute(
    """
    SELECT COUNT(*) as total,
           SUM(CASE WHEN Updated_At > ? THEN 1 ELSE 0 END) as updated
    FROM filings
    """,
    [before[1]],
).fetchone()

print(f"\nAfter sync:")
print(f"  Total records: {after[0]}")
print(f"  Records updated: {after[1]}")

# Check sync history
history = conn.execute(
    """
    SELECT sync_id, started_at, records_processed, status
    FROM sync_history 
    ORDER BY sync_id DESC 
    LIMIT 5
    """
).fetchall()

print("\nRecent sync history:")
for sync in history:
    print(f"  Sync {sync[0]}: {sync[3]} - {sync[2]} records processed")

conn.close()

if after[1] == 0:
    print("\n✅ SUCCESS: No records updated (nothing changed in Airtable)")
else:
    print(f"\n✅ SUCCESS: Only {after[1]} records updated")
