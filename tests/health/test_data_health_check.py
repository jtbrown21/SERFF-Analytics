from serff_analytics.db import DatabaseManager
from src.data_health_check import SimpleDataHealthCheck


def test_missing_wa_2024_detected(tmp_path):
    db_file = tmp_path / "filings.db"
    manager = DatabaseManager(str(db_file))
    conn = manager.get_connection()
    checker = SimpleDataHealthCheck(str(db_file))
    for abbr in checker.ALL_STATES:
        if abbr == "WA":
            continue
        full = next(k for k, v in checker.NAME_TO_ABBR.items() if v == abbr)
        conn.execute(
            "INSERT INTO filings (Record_ID, Company, State, Product_Line, Rate_Change_Type, Effective_Date) VALUES (?, ?, ?, ?, ?, ?)",
            (f"id_{abbr}", "TestCo", full, "Auto", "increase", "2024-01-01"),
        )
    conn.close()
    result = checker.check_state_filing_completeness(2024, 2024)
    assert {"state": "WA", "year": 2024} in result["missing"]
