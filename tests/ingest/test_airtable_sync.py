import datetime
from unittest.mock import MagicMock

import pytest
from pyairtable import Table

from serff_analytics.ingest.airtable_sync import AirtableSync
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

    mock_iterate = MagicMock(return_value=iter(pages))
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


def test_sync_fails_on_duplicate_ids(monkeypatch, db_path):
    pages = [[make_record("dup", 0.1), make_record("dup", 0.1)]]

    mock_iterate = MagicMock(return_value=iter(pages))
    monkeypatch.setattr(Table, "iterate", mock_iterate)
    monkeypatch.setattr(config.Config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config.Config, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(config.Config, "AIRTABLE_BASE_ID", "base")
    monkeypatch.setattr(config.Config, "AIRTABLE_TABLE_NAME", "table")

    sync = AirtableSync()
    result = sync.sync_data()

    assert not result["success"]
    with sync.db.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    assert count == 0


def test_temp_table_removed(monkeypatch, db_path):
    pages = [[make_record("t1", 0.1)]]

    mock_iterate = MagicMock(return_value=iter(pages))
    monkeypatch.setattr(Table, "iterate", mock_iterate)
    monkeypatch.setattr(config.Config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config.Config, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(config.Config, "AIRTABLE_BASE_ID", "base")
    monkeypatch.setattr(config.Config, "AIRTABLE_TABLE_NAME", "table")

    sync = AirtableSync()
    sync.sync_data()

    with sync.db.connection() as conn:
        tmp_exists = conn.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name='tmp_filings'"
        ).fetchone()[0]
        assert tmp_exists == 0


def test_sync_fails_when_duplicate_ids_across_pages(monkeypatch, db_path):
    pages = [
        [make_record("upd", 0.1)],
        [make_record("upd", 0.2)],
    ]

    mock_iterate = MagicMock(return_value=iter(pages))
    monkeypatch.setattr(Table, "iterate", mock_iterate)
    monkeypatch.setattr(config.Config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config.Config, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(config.Config, "AIRTABLE_BASE_ID", "base")
    monkeypatch.setattr(config.Config, "AIRTABLE_TABLE_NAME", "table")

    sync = AirtableSync()
    result = sync.sync_data()

    assert not result["success"]
    with sync.db.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM filings WHERE Record_ID='upd'").fetchone()[0]
    assert count == 0


def test_sync_uses_single_transaction(monkeypatch, db_path):
    pages = [[make_record("a", 0.1)], [make_record("b", 0.2)]]

    mock_iterate = MagicMock(return_value=iter(pages))
    monkeypatch.setattr(Table, "iterate", mock_iterate)
    monkeypatch.setattr(config.Config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config.Config, "AIRTABLE_API_KEY", "key")
    monkeypatch.setattr(config.Config, "AIRTABLE_BASE_ID", "base")
    monkeypatch.setattr(config.Config, "AIRTABLE_TABLE_NAME", "table")

    sync = AirtableSync()

    calls = []
    orig_trans = sync.db.transaction

    def counting_trans(*args, **kwargs):
        calls.append(1)
        return orig_trans(*args, **kwargs)

    monkeypatch.setattr(sync.db, "transaction", counting_trans)

    result = sync.sync_data()

    assert result["records_inserted"] == 2
    assert len(calls) == 1
