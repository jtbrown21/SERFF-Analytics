from dotenv import load_dotenv
import os
import sys
from src.report_manager import ReportManager
import logging
from src.email_service import (
    send_newsletter_embedded_with_subscriber_tracking,
    get_test_subscribers,
    get_subscribers_by_state,
)
from src.shared.utils import (
    get_current_month_year,
    check_required_env_vars,
)

load_dotenv()
logger = logging.getLogger(__name__)

if not check_required_env_vars():
    logger.error("Required environment variables not set. Aborting.")
    sys.exit(1)


def _get_recipients(state: str, test_mode: bool = True):
    if test_mode:
        subs = get_test_subscribers()
    else:
        subs = get_subscribers_by_state(state)
    emails = [s["fields"].get("Email") for s in subs if s["fields"].get("Email")]
    return emails


def send_approved_reports(dry_run: bool = False, test_mode: bool = True):
    """Send all approved reports with full HTML embedded"""
    month, year = get_current_month_year()
    manager = ReportManager()

    print(f"=== Sending {month} {year} Approved Reports ===\n")

    approved = manager.get_approved_reports(month, year)

    if not approved:
        print("❌ No approved reports found")
        return

    print(f"Found {len(approved)} approved report(s)\n")

    for report in approved:
        fields = report["fields"]
        state = fields["State"]

        print(f"📧 {fields['Name']}:")

        report_path = (
            f"docs/reports/{year}-{month.lower()[:3]}/{state.lower().replace(' ', '-')}.html"
        )

        if not os.path.exists(report_path):
            print(f"  ❌ Report file not found: {report_path}")
            continue

        recipients = _get_recipients(state, test_mode=test_mode)

        if not dry_run:
            try:
                send_newsletter_embedded_with_subscriber_tracking(
                    state=state,
                    month=fields["Month"],
                    year=fields["Year"],
                    report_path=report_path,
                    report_record_id=report["id"],
                    test_mode=test_mode,
                )

                manager.mark_as_sent(report["id"])
                logging.info("Sent report for %s to %d recipients", state, len(recipients))

            except Exception as e:
                logging.error("Failed to send report for %s: %s", state, e)
        else:
            print(f"  [DRY RUN] Would embed and send {report_path}")
