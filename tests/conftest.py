import pytest
from serff_analytics.db import DatabaseManager


@pytest.fixture
def db_path(tmp_path):
    """Return path to a temporary database file."""
    return tmp_path / "test.db"


@pytest.fixture
def db_manager(db_path):
    """Return a DatabaseManager connected to the temporary database."""
    return DatabaseManager(str(db_path))
