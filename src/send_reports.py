from dotenv import load_dotenv
import os
from src.report_manager import ReportManager
import logging
from src.email_service import (
    send_newsletter_embedded_with_subscriber_tracking,
    get_test_subscribers,
    get_subscribers_by_state,
)
from src.shared.utils import get_current_month_year

load_dotenv()
logger = logging.getLogger(__name__)

# Configure module level logging
if not logger.handlers:
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    file_handler = logging.FileHandler("send_reports.log")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


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

    logger.info("=== Sending %s %s Approved Reports ===", month, year)

    approved = manager.get_approved_reports(month, year)

    if not approved:
        logger.error("❌ No approved reports found")
        return

    logger.info("Found %d approved report(s)", len(approved))

    for report in approved:
        fields = report["fields"]
        state = fields["State"]

        logger.info("📧 %s:", fields["Name"])

        report_path = (
            f"docs/reports/{year}-{month.lower()[:3]}/{state.lower().replace(' ', '-')}.html"
        )

        if not os.path.exists(report_path):
            logger.error("  ❌ Report file not found: %s", report_path)
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

            except Exception:
                logger.exception("Failed to send report for %s", state)
        else:
            logger.info("  [DRY RUN] Would embed and send %s", report_path)
