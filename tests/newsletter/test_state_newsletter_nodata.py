import pytest
from serff_analytics.reports.state_newsletter import StateNewsletterReport, NoDataError
from serff_analytics.db import DatabaseManager


def test_generate_with_no_data(tmp_path):
    db_file = tmp_path / "empty.db"
    DatabaseManager(str(db_file))
    report = StateNewsletterReport(db_path=str(db_file))
    with pytest.raises(NoDataError):
        report.generate("TX", "2024-01")
