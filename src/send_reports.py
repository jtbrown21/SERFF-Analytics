from dotenv import load_dotenv
import os
from src.report_manager import ReportManager
import logging
from src.email_service import (
    send_newsletter_embedded_with_subscriber_tracking,
    get_test_subscribers,
    get_subscribers_by_state,
)

load_dotenv()
logger = logging.getLogger(__name__)



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

    base_dir = Path(os.getenv("NEWSLETTERS_DIR", "docs/newsletters/monthly/19.0"))

    for report in approved:
        fields = report["fields"]
        state = fields["State"]

        logger.info("📧 %s:", fields["Name"])

        state_abbr = normalize_state_abbr(state)
        month_field = fields.get("Month", month)
        try:
            month_dt = datetime.strptime(month_field, "%B")
        except ValueError:
            try:
                month_dt = datetime.strptime(month_field, "%b")
            except ValueError:
                month_dt = datetime.strptime(month_field, "%m")

        month_num = month_dt.strftime("%m")
        month_full = month_dt.strftime("%B")
        filename = f"{state_abbr}_{month_num}_{year}.html"
        report_path = base_dir / state_abbr / year / month_full / filename

            continue

        recipients = _get_recipients(state, test_mode=test_mode)

        if not dry_run:
            try:
                send_newsletter_embedded_with_subscriber_tracking(
                    state=state,
                    month=fields["Month"],
                    year=fields["Year"],
                    report_path=str(report_path),
                    report_record_id=report["id"],
                    test_mode=test_mode,
                )

                manager.mark_as_sent(report["id"])
                logging.info("Sent report for %s to %d recipients", state, len(recipients))

            except Exception:
                logger.exception("Failed to send report for %s", state)
        else:
            logger.info("  [DRY RUN] Would embed and send %s", report_path)
