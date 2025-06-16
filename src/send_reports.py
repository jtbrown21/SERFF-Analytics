from dotenv import load_dotenv
import os
from pathlib import Path
from datetime import datetime
from src.report_manager import ReportManager
import logging
from src.email_service import (
    send_newsletter_embedded_with_subscriber_tracking,
    get_test_subscribers,
    get_subscribers_by_state,
)
from src.shared.utils import get_current_month_year
from serff_analytics.reports.state_newsletter import normalize_state_abbr

load_dotenv()
logger = logging.getLogger(__name__)


def _get_recipients(state: str, test_mode: bool = True):
    if test_mode:
        subs = get_test_subscribers()
    else:
        subs = get_subscribers_by_state(state)
    emails = [s["fields"].get("Email") for s in subs if s["fields"].get("Email")]
    return emails


def send_approved_reports(
    dry_run: bool = False,
    test_mode: bool = False,
    test_item: str | None = None,
):
    """Send all approved reports with full HTML embedded.

    When ``test_mode`` is enabled only a single email is sent. The optional
    ``test_item`` argument specifies which state's report to send. If omitted,
    the first approved report is used.
    """
    month, year = get_current_month_year()
    manager = ReportManager()

    logger.info("=== Sending %s %s Approved Reports ===", month, year)

    approved = manager.get_approved_reports(month, year)

    if not approved:
        logger.error("❌ No approved reports found")
        return

    logger.info("Found %d approved report(s)", len(approved))

    reports = approved
    if test_mode:
        if test_item:
            reports = [r for r in approved if r["fields"].get("State") == test_item]
            if not reports:
                logger.error("TEST MODE: No report found for %s", test_item)
                return
        else:
            reports = [approved[0]]
        logger.info("TEST MODE: Sending only to recipient %s", reports[0]["fields"].get("State"))

    prefix = "[TEST] " if test_mode else ""

    base_dir = Path(os.getenv("NEWSLETTERS_DIR", "docs/newsletters/monthly/19.0"))

    for report in reports:
        fields = report["fields"]
        state = fields["State"]

        logger.info("%s📧 %s:", prefix, fields["Name"])

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

        recipients = _get_recipients(state, test_mode=test_mode)
        if test_mode and recipients:
            recipients = [test_item] if test_item else [recipients[0]]

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
                logging.info(
                    "%sSent report for %s to %d recipients", prefix, state, len(recipients)
                )

            except Exception:
                logger.exception("%sFailed to send report for %s", prefix, state)
        else:
            logger.info("%s  [DRY RUN] Would embed and send %s", prefix, report_path)
