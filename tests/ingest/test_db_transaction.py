import pytest

from serff_analytics.db import DatabaseManager


def test_transaction_commit(db_manager):
    with db_manager.connection() as conn:
        conn.execute("CREATE TABLE test (id INTEGER)")

    with db_manager.transaction() as conn:
        conn.execute("INSERT INTO test VALUES (1)")

    with db_manager.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
    assert count == 1


def test_transaction_rollback(db_manager):
    with db_manager.connection() as conn:
        conn.execute("CREATE TABLE test (id INTEGER)")

    with pytest.raises(ValueError):
        with db_manager.transaction() as conn:
            conn.execute("INSERT INTO test VALUES (1)")
            raise ValueError("boom")

    with db_manager.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
    assert count == 0
