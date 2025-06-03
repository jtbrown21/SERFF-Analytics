"""Generate monthly HTML reports for all active states."""

import logging
import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv

from src.report_manager import ReportManager
from src.shared.utils import ALL_STATES, get_current_month_year
from serff_analytics.reports.state_newsletter import StateNewsletterReport
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
                output_dir = f"docs/reports/{year}-{month.lower()[:3]}"
                os.makedirs(output_dir, exist_ok=True)
                filename = f"{state.lower().replace(' ', '-')}.html"
                output_path = os.path.join(output_dir, filename)

                reporter = StateNewsletterReport()
                month_num = datetime.strptime(month, "%B").month
                month_tag = f"{year}-{month_num:02d}"
                html = reporter.generate(state, month_tag)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(html)

                report_url = (
                    f"https://{os.getenv('GITHUB_USERNAME','USERNAME')}.github.io/"
                    f"{os.getenv('GITHUB_REPO_NAME','SERFF-Analytics')}/reports/"
                    f"{year}-{month.lower()[:3]}/{filename}"
                )

                manager.log_report(state=state, month=month, year=year, report_url=report_url)

                print(" ✓")
                generated_count += 1
            else:
                print(" [DRY RUN]")

        except Exception as e:
            print(f" ❌ Error: {e}")

    print(f"\n✓ Generated {generated_count} reports")

    if generated_count > 0 and not dry_run:
        print("\n📤 Pushing to GitHub Pages...")
        subprocess.run(["git", "add", "docs/reports/"], check=False)
        subprocess.run(["git", "commit", "-m", f"Add {month} {year} reports"], check=False)
        subprocess.run(["git", "push"], check=False)
        print("✓ Pushed to GitHub")
