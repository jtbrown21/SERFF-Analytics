import pytest
import duckdb
from serff_analytics.db import DatabaseManager


@pytest.fixture
def db_path(tmp_path):
    """Return path to a temporary database file with initialized schema."""
    db_file = tmp_path / "test.db"
    
    # Initialize the database with required tables
    conn = duckdb.connect(str(db_file))
    
    # Create sync_history table with exact production schema
    conn.execute("""
        CREATE TABLE sync_history(
            sync_id INTEGER,
            started_at TIMESTAMP DEFAULT(CURRENT_TIMESTAMP),
            completed_at TIMESTAMP,
            records_processed INTEGER DEFAULT(0),
            records_inserted INTEGER DEFAULT(0),
            records_updated INTEGER DEFAULT(0),
            records_skipped INTEGER DEFAULT(0),
            parsing_errors INTEGER DEFAULT(0),
            status VARCHAR DEFAULT('running'),
            sync_mode VARCHAR DEFAULT('unknown'),
            UNIQUE(sync_id)
        )
    """)
    
    # Create filings table with exact production schema
    conn.execute("""
        CREATE TABLE filings(
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
            Created_At TIMESTAMP DEFAULT(CURRENT_TIMESTAMP),
            Updated_At TIMESTAMP DEFAULT(CURRENT_TIMESTAMP),
            Airtable_Last_Modified TIMESTAMP,
            UNIQUE(Record_ID)
        )
    """)
    
    conn.close()
    
    return db_file


@pytest.fixture
def db_manager(db_path):
    """Return a DatabaseManager connected to the temporary database."""
    return DatabaseManager(str(db_path))