import os
import pytest
from serff_analytics.reports.state_newsletter import StateNewsletterReport


def test_missing_db_raises(tmp_path):
    db_file = tmp_path / "missing.db"
    report = StateNewsletterReport(db_path=str(db_file))
    with pytest.raises(FileNotFoundError):
        report._get_connection()
