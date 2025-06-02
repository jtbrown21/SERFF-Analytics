import os
from serff_analytics.reports.state_newsletter import StateNewsletterReport


def test_db_auto_created(tmp_path):
    db_file = tmp_path / "auto.db"
    report = StateNewsletterReport(db_path=str(db_file))
    conn = report._get_connection()
    assert db_file.exists()
    conn.close()
