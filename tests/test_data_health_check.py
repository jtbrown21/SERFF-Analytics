import pandas as pd
from src.data_health_check import check_state_filing_completeness, SimpleDataHealthCheck


def test_missing_wa_2024_detected():
    checker = SimpleDataHealthCheck()
    data = [
        {"state_code": state, "year": 2024, "filing_status": "submitted"}
        for state in checker.ALL_STATES
        if state != "WA"
    ]
    missing = check_state_filing_completeness(data, start_year=2024, end_year=2024)
    assert {"state": "WA", "year": 2024} in missing


def test_invalid_status_counts_as_missing():
    checker = SimpleDataHealthCheck()
    data = [
        {"state_code": state, "year": 2024, "filing_status": "submitted"}
        for state in checker.ALL_STATES
    ]
    for row in data:
        if row["state_code"] == "WA":
            row["filing_status"] = ""
    missing = check_state_filing_completeness(data, start_year=2024, end_year=2024)
    assert {"state": "WA", "year": 2024} in missing
