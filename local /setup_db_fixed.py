import duckdb
import os

print("Setting up database with verified workaround...")

# Remove existing database
if os.path.exists('serff_analytics/data/insurance_filings.db'):
    os.remove('serff_analytics/data/insurance_filings.db')
    print("Removed existing database")

# Create new database
conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

# Create filings table WITHOUT PRIMARY KEY constraint
# Use UNIQUE + NOT NULL instead (functionally equivalent)
print("Creating filings table...")
conn.execute("""
    CREATE TABLE filings (
        Record_ID VARCHAR NOT NULL,
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
        Airtable_Last_Modified TIMESTAMP,
        UNIQUE (Record_ID)
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
        sync_id INTEGER,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        records_processed INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        records_skipped INTEGER DEFAULT 0,
        parsing_errors INTEGER DEFAULT 0,
        status VARCHAR(50) DEFAULT 'running',
        sync_mode VARCHAR(50) DEFAULT 'unknown',
        UNIQUE (sync_id)
    )
""")

# Create sequence
conn.execute("CREATE SEQUENCE sync_history_seq START 1")

# Test that constraints work correctly
print("\nTesting constraints...")
try:
    # Test insert
    conn.execute("""
        INSERT INTO filings (Record_ID, Company, State) 
        VALUES ('test123', 'Test Company', 'CA')
    """)
    print("✅ Insert successful")
    
    # Test duplicate rejection
    try:
        conn.execute("""
            INSERT INTO filings (Record_ID, Company, State) 
            VALUES ('test123', 'Different Company', 'NY')
        """)
        print("❌ UNIQUE constraint not working!")
    except Exception as e:
        print("✅ UNIQUE constraint working (duplicate rejected)")
    
    # Test update
    conn.execute("""
        UPDATE filings SET Company = 'Updated Company' 
        WHERE Record_ID = 'test123'
    """)
    print("✅ Update successful")
    
    # Clean up test
    conn.execute("DELETE FROM filings WHERE Record_ID = 'test123'")
    
except Exception as e:
    print(f"❌ Test failed: {e}")

# Verify final structure
print("\nVerifying final structure...")
tables = conn.execute("SHOW TABLES").fetchall()
print(f"Tables: {[t[0] for t in tables]}")

conn.close()
print("\n✅ Database setup complete!")