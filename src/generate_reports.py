"""Generate monthly HTML reports for all active states."""

import logging
import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv

from src.report_manager import ReportManager
from src.shared.utils import ALL_STATES, get_current_month_year
from pathlib import Path
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

    print(f"=== Generating {month} {year} Reports ===\n")

    generated_count = 0

    base_dir = Path(os.getenv("NEWSLETTERS_DIR", "docs/newsletters/monthly/19.0"))

    for state in ALL_STATES:
        existing = manager.get_report_by_state_month_year(state, month, year)
        if existing:
            print(f"⏭️  {state}: Report already exists")
            continue

        if not _state_has_activity(state, month, year):
            print(f"⏭️  {state}: No activity this month")
            continue

        try:
            print(f"📄 Generating {state}...", end="", flush=True)

            if not dry_run:
                state_abbr = normalize_state_abbr(state)
                month_num = datetime.strptime(month, "%B").month
                month_tag = f"{year}-{month_num:02d}"
                month_full = datetime.strptime(month, "%B").strftime("%B")

                reporter = StateNewsletterReport()
                html = reporter.generate(state, month_tag)

                output_dir = base_dir / state_abbr / year / month_full
                output_dir.mkdir(parents=True, exist_ok=True)
                filename = f"{state_abbr}_{month_num:02d}_{year}.html"
                output_path = output_dir / filename

                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(html)

                report_url = (
                    f"https://{os.getenv('GITHUB_USERNAME','USERNAME')}.github.io/"
                    f"{os.getenv('GITHUB_REPO_NAME','SERFF-Analytics')}/newsletters/"
                    f"monthly/19.0/{state_abbr}/{year}/{month_full}/{filename}"
                )

                manager.log_report(
                    state=state,
                    month=month,
                    year=year,
                    report_url=report_url,
                    filings=reporter.last_filing_ids,
                )

                print(" ✓")
                generated_count += 1
            else:
                print(" [DRY RUN]")

        except Exception as e:
            print(f" ❌ Error: {e}")

    print(f"\n✓ Generated {generated_count} reports")

    if generated_count > 0 and not dry_run:
        print("\n📤 Pushing to GitHub Pages...")
        subprocess.run(["git", "add", str(base_dir)], check=False)
        subprocess.run(["git", "commit", "-m", f"Add {month} {year} reports"], check=False)
        subprocess.run(["git", "push"], check=False)
        print("✓ Pushed to GitHub")
