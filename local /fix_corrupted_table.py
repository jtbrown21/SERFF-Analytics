import duckdb
import pandas as pd

print("Fixing corrupted filings table...")

# Connect to database
conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

try:
    # 1. Check existing columns
    print("1. Checking existing table structure...")
    existing_cols = conn.execute("PRAGMA table_info(filings)").fetchall()
    existing_col_names = [col[1] for col in existing_cols]
    print(f"   Current columns ({len(existing_col_names)}): {', '.join(existing_col_names[:5])}...")
    
    # 2. Backup existing data
    print("2. Backing up existing data...")
    backup_df = conn.execute("SELECT * FROM filings").fetchdf()
    print(f"   Backed up {len(backup_df)} records with {len(backup_df.columns)} columns")
    
    # 3. Drop the corrupted table
    print("3. Dropping corrupted table...")
    conn.execute("DROP TABLE IF EXISTS filings")
    
    # 4. Recreate table with proper schema
    print("4. Creating new table with correct schema...")
    conn.execute("""
        CREATE TABLE filings(
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
    
    # 5. Recreate indexes
    print("5. Creating indexes...")
    conn.execute("CREATE INDEX idx_company ON filings(Company, Subsidiary)")
    conn.execute("CREATE INDEX idx_effective_date ON filings(Effective_Date)")
    conn.execute("CREATE INDEX idx_state_product ON filings(State, Product_Line)")
    
    # 6. Restore data if any existed
    if len(backup_df) > 0:
        print("6. Restoring data...")
        
        # Add missing column if it doesn't exist
        if 'Airtable_Last_Modified' not in backup_df.columns:
            print("   Adding Airtable_Last_Modified column to backup data...")
            backup_df['Airtable_Last_Modified'] = None
        
        # Ensure column order matches new table
        new_cols = ['Record_ID', 'Company', 'Subsidiary', 'State', 'Product_Line',
                    'Rate_Change_Type', 'Premium_Change_Number', 'Premium_Change_Amount_Text',
                    'Effective_Date', 'Previous_Increase_Date', 'Previous_Increase_Number',
                    'Policyholders_Affected_Number', 'Policyholders_Affected_Text',
                    'Total_Written_Premium_Number', 'Total_Written_Premium_Text',
                    'SERFF_Tracking_Number', 'Specific_Coverages', 'Stated_Reasons',
                    'Population', 'Impact_Score', 'Renewals_Date', 'Created_At',
                    'Updated_At', 'Airtable_Last_Modified']
        
        # Reorder columns to match
        backup_df = backup_df[new_cols]
        
        conn.register('backup_data', backup_df)
        conn.execute("INSERT INTO filings SELECT * FROM backup_data")
        conn.unregister('backup_data')
        print(f"   Restored {len(backup_df)} records")
    else:
        print("6. No data to restore")
    
    # 7. Verify fix
    print("7. Verifying fix...")
    
    # Check constraints
    constraints = conn.execute("""
        SELECT * FROM duckdb_constraints 
        WHERE table_name = 'filings'
    """).fetchall()
    
    correct_pk = False
    for c in constraints:
        if 'PRIMARY KEY' in str(c):
            pk_count = str(c).count('Record_ID')
            if pk_count == 1:
                print("   ✅ PRIMARY KEY constraint is correct")
                correct_pk = True
            else:
                print(f"   ❌ PRIMARY KEY has {pk_count} Record_ID references (should be 1)")
    
    # Check final column count
    final_cols = conn.execute("PRAGMA table_info(filings)").fetchall()
    print(f"   ✅ Table has {len(final_cols)} columns")
    
    # Check row count
    row_count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    print(f"   ✅ Table has {row_count} rows")
    
    conn.close()
    
    if correct_pk:
        print("\n✅ Table recreation complete! You can now run sync.")
    else:
        print("\n⚠️  Table recreated but PRIMARY KEY may still have issues.")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    conn.close()
    raise