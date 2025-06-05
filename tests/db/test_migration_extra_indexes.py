import duckdb
from serff_analytics.db import DatabaseManager

SCHEMA = """
CREATE TABLE filings (
    Record_ID VARCHAR PRIMARY KEY,
    Company VARCHAR,
    Subsidiary VARCHAR,
    State VARCHAR,
    Product_Line VARCHAR,
    Rate_Change_Type VARCHAR,
    Premium_Change_Number DECIMAL(10,2),
    Premium_Change_Amount_Text VARCHAR,
    Effective_Date DATE,
    Previous_Increase_Date DATE,
    Previous_Increase_Percentage DECIMAL(10,2),
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
    Updated_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def test_migration_drops_extra_indexes(tmp_path):
    db_file = tmp_path / "old.db"
    with duckdb.connect(str(db_file)) as conn:
        conn.execute(SCHEMA)
        conn.execute("CREATE INDEX idx_extra ON filings(Premium_Change_Number)")
        conn.execute(
            "CREATE INDEX idx_filings_state_year ON filings(State, YEAR(Effective_Date))"
        )

    # Should run without raising an error
    DatabaseManager(str(db_file))

    with duckdb.connect(str(db_file)) as conn:
        info = {
            row[1]: row[2]
            for row in conn.execute("PRAGMA table_info('filings')").fetchall()
        }
        assert info["Premium_Change_Number"].upper() == "DECIMAL(10,4)"
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name='filings'"
            ).fetchall()
        }

    assert "idx_extra" not in indexes
    assert "idx_filings_state_year" not in indexes
