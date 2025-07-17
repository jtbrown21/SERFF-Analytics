import duckdb
import os

print("Setting up fresh database...")

# Ensure directory exists
os.makedirs('serff_analytics/data', exist_ok=True)

# Create new database
conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

# Create filings table
print("Creating filings table...")
conn.execute("""
    CREATE TABLE filings (
        Record_ID VARCHAR PRIMARY KEY,
        Company VARCHAR,
        Subsidiary VARCHAR,
        State VARCHAR,
        Product_Line VARCHAR,
        Rate_Change_Type VARCHAR,
        Premium_Change_Number DECIMAL(10,4),
        Premium_Change_Amount_Text VARCHAR,
        Effective_Date DATE,
        Previous_Increase_Date DATE,
        Previous_Increase_Number DECIMAL(10,4),
        Policyholders_Affected_Number INTEGER,
        Policyholders_Affected_Text VARCHAR,
        Total_Written_Premium_Number DECIMAL(15,2),
        Total_Written_Premium_Text VARCHAR,
        SERFF_Tracking_Number VARCHAR,
        Specific_Coverages VARCHAR,
        Stated_Reasons VARCHAR,
        Population VARCHAR,
        Impact_Score DECIMAL(10,2),
        Renewals_Date DATE,
        Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        Updated_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        Airtable_Last_Modified TIMESTAMP
    )
""")

# Create indexes
print("Creating indexes...")
conn.execute("CREATE INDEX idx_company ON filings (Company, Subsidiary)")
conn.execute("CREATE INDEX idx_effective_date ON filings (Effective_Date)")
conn.execute("CREATE INDEX idx_state_product ON filings (State, Product_Line)")

# Create sync_history table
print("Creating sync_history table...")
conn.execute("""
    CREATE TABLE sync_history (
        sync_id INTEGER PRIMARY KEY,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        records_processed INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        records_skipped INTEGER DEFAULT 0,
        parsing_errors INTEGER DEFAULT 0,
        status VARCHAR(50) DEFAULT 'running',
        sync_mode VARCHAR(50) DEFAULT 'unknown'
    )
""")

# Create sequence for sync_history
conn.execute("CREATE SEQUENCE sync_history_seq START 1")

# Verify
print("\nVerifying setup...")
tables = conn.execute("SHOW TABLES").fetchall()
print(f"Tables created: {[t[0] for t in tables]}")

# Check PRIMARY KEY is correct
constraints = conn.execute("""
    SELECT * FROM duckdb_constraints 
    WHERE table_name = 'filings' AND constraint_type = 'PRIMARY KEY'
""").fetchall()

if constraints:
    pk_str = str(constraints[0])
    record_id_count = pk_str.count('Record_ID')
    if record_id_count == 1:
        print("✅ PRIMARY KEY is correct!")
    else:
        print(f"❌ PRIMARY KEY still has issues: {record_id_count} Record_ID references")
else:
    print("❌ No PRIMARY KEY constraint found")

conn.close()
print("\nDatabase setup complete!")