"""Generate monthly HTML reports for all active states."""

import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from src.report_manager import ReportManager
from src.shared.utils import ALL_STATES, get_current_month_year
from serff_analytics.reports.state_newsletter import (
    StateNewsletterReport,
    normalize_state_abbr,
)
from serff_analytics.db.utils import get_month_boundaries
from serff_analytics.db import DatabaseManager
from serff_analytics.config import Config

load_dotenv()
logger = logging.getLogger(__name__)

def _state_has_activity(state: str, month: str, year: str) -> bool:
    """Return True if filings exist for this state in the given period."""
    try:
        db = DatabaseManager(Config.DB_PATH)
        conn = db.get_connection()
        month_num = datetime.strptime(month, "%B").month
        start, end = get_month_boundaries(int(year), month_num)
        query = (
            "SELECT COUNT(*) FROM filings "
            "WHERE State = ? AND Effective_Date >= ? AND Effective_Date <= ?"
        )
        count = conn.execute(query, [state, start.isoformat(), end.isoformat()]).fetchone()[0]
        conn.close()
        return count > 0
    except Exception as exc:
        logger.error("Activity check failed for %s: %s", state, exc)
        return False


def generate_all_reports(dry_run: bool = False) -> None:
    """Generate reports for all states with activity"""
    month, year = get_current_month_year()
    manager = ReportManager()

    logger.info("=== Generating %s %s Reports ===", month, year)

    generated_count = 0

    base_dir = Path(os.getenv("NEWSLETTERS_DIR", "docs/newsletters/monthly/19.0"))

    for state in ALL_STATES:
        existing = manager.get_report_by_state_month_year(state, month, year)
        if existing:
            logger.info("⏭️  %s: Report already exists", state)
            continue

        if not _state_has_activity(state, month, year):
            logger.info("⏭️  %s: No activity this month", state)
            continue

        try:
            logger.info("📄 Generating %s...", state)

            if not dry_run:
                state_abbr = normalize_state_abbr(state)
                month_num = datetime.strptime(month, "%B").month
                month_tag = f"{year}-{month_num:02d}"
                month_full = datetime.strptime(month, "%B").strftime("%B")

                reporter = StateNewsletterReport()
                html = reporter.generate(state, month_tag)

                filename = f"{state_abbr}_{month_num:02d}_{year}.html"
                output_dir = base_dir / state_abbr / year / month_full
                outfile = reporter.save(html, filename, output_dir=str(output_dir))

                report_url = (
                    f"https://{os.getenv('GITHUB_USERNAME','USERNAME')}.github.io/"
                    f"{os.getenv('GITHUB_REPO_NAME','SERFF-Analytics')}/newsletters/"
                    f"monthly/19.0/{state_abbr}/{year}/{month_full}/{filename}"
                )

                subprocess.run(["git", "add", outfile], check=False)

                try:
                    manager.log_report(
                        state=state,
                        month=month,
                        year=year,
                        report_url=report_url,
                        filings=reporter.last_filing_ids,
                    )
                except Exception:
                    logger.exception("Failed to log report for %s", state)

                logger.info("✓ Generated %s", state)
                generated_count += 1
            else:
                logger.info("[DRY RUN] Would generate %s", state)

        except Exception:
            logger.exception("Error generating report for %s", state)

    logger.info("✓ Generated %d reports", generated_count)

    if generated_count > 0 and not dry_run:
        subprocess.run(["git", "commit", "-m", f"Add {month} {year} reports"], check=False)
        subprocess.run(["git", "push"], check=False)
        logger.info("✓ Pushed to GitHub")
