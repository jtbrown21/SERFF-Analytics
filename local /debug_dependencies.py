import duckdb

conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

# Check indexes
print("INDEXES:")
indexes = conn.execute("""
    SELECT * FROM duckdb_indexes 
    WHERE table_name = 'filings'
""").fetchall()
for idx in indexes:
    print(f"  {idx}")

# Check constraints
print("\nCONSTRAINTS:")
constraints = conn.execute("""
    SELECT * FROM duckdb_constraints 
    WHERE table_name = 'filings'
""").fetchall()
for const in constraints:
    print(f"  {const}")

# Check column info
print("\nCOLUMNS:")
cols = conn.execute("PRAGMA table_info(filings)").fetchall()
for col in cols:
    print(f"  {col[1]} - {col[2]}")

conn.close()