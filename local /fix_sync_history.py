# Create fix_sync_history.py
import duckdb

conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

# Fix existing records
conn.execute("""
    UPDATE sync_history 
    SET sync_mode = CASE 
        WHEN sync_id = 1 THEN 'full'
        ELSE 'incremental'
    END
    WHERE sync_mode = 'unknown'
""")

print("Updated sync history modes")

# Verify
results = conn.execute("""
    SELECT sync_id, sync_mode, records_processed 
    FROM sync_history 
    ORDER BY sync_id
""").fetchall()

for r in results:
    print(f"Sync {r[0]}: mode={r[1]}, records={r[2]}")

conn.close()