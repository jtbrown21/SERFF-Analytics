import datetime
from unittest.mock import MagicMock

import pytest
from pyairtable import Table

from serff_analytics.ingest.airtable_sync import AirtableSync
from serff_analytics.db import DatabaseManager
from serff_analytics import config


def make_record(record_id, rate_change):
    return {
        "id": record_id,
        "fields": {
            "Parent Company": "PCo",
            "Filing Company": "FCo",
            "Impacted State": "TX",
            "Product Line": "Auto",
            "Rate Change Type": "increase",
            "Overall Rate Change Number": rate_change,
        },
    }


def test_sync_only_updated_records(monkeypatch, db_path):
    since = datetime.datetime(2024, 1, 1, 0, 0, 0)
    pages = [[make_record("r1", 0.1)], [make_record("r2", 0.2)]]

    mock_iterate = MagicMock(return_value=pages)
    monkeypatch.setattr(Table, "iterate", mock_iterate)
    monkeypatch.setattr(config.Config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config.Config, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(config.Config, "AIRTABLE_BASE_ID", "base")
    monkeypatch.setattr(config.Config, "AIRTABLE_TABLE_NAME", "table")

    sync = AirtableSync()
    result = sync.sync_data(since=since)

    mock_iterate.assert_called_once()
    args, kwargs = mock_iterate.call_args
    assert kwargs["page_size"] == 100
    assert "filter_by_formula" in kwargs
    assert since.isoformat() in kwargs["filter_by_formula"]

    assert result["records_inserted"] == 2
    with sync.db.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    assert count == 2


def test_sync_deduplicates_record_id(monkeypatch, db_path):
    pages = [[make_record("dup", 0.1), make_record("dup", 0.1)]]

    mock_iterate = MagicMock(return_value=pages)
    monkeypatch.setattr(Table, "iterate", mock_iterate)
    monkeypatch.setattr(config.Config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config.Config, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(config.Config, "AIRTABLE_BASE_ID", "base")
    monkeypatch.setattr(config.Config, "AIRTABLE_TABLE_NAME", "table")

    sync = AirtableSync()
    result = sync.sync_data()

    assert result["records_inserted"] == 1
    with sync.db.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    assert count == 1
