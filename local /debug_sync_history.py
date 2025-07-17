# Create debug_sync_history.py
import duckdb
from datetime import datetime

conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

print("=== SYNC HISTORY ===")
history = conn.execute("""
    SELECT sync_id, started_at, completed_at, sync_mode, status, records_processed
    FROM sync_history 
    ORDER BY started_at DESC 
    LIMIT 5
""").fetchall()

for h in history:
    print(f"ID: {h[0]}, Started: {h[1]}, Completed: {h[2]}, Mode: {h[3]}, Status: {h[4]}, Records: {h[5]}")

print("\n=== LAST SUCCESSFUL SYNC ===")
last_success = conn.execute("""
    SELECT sync_id, completed_at, sync_mode 
    FROM sync_history 
    WHERE status = 'completed' 
    ORDER BY completed_at DESC 
    LIMIT 1
""").fetchone()

if last_success:
    print(f"Found: ID {last_success[0]}, Completed: {last_success[1]}, Mode: {last_success[2]}")
else:
    print("NO SUCCESSFUL SYNC FOUND!")

conn.close()