import os
import json
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import List

import pytest
import requests
from dotenv import load_dotenv

from src.report_manager import ReportManager
from src.email_service import (
    send_newsletter_embedded_with_subscriber_tracking,
    get_test_subscribers,
)
from src.shared.utils import get_current_month_year


class EmailSendError(Exception):
    """Raised when an email fails to send."""


class WebhookCallError(Exception):
    """Raised when a webhook call fails."""


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler(
    "test_send_approved.log", maxBytes=1024 * 1024, backupCount=3
)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(funcName)s: %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


@pytest.fixture(scope="module")
def manager() -> ReportManager:
    """Initialize ReportManager and load environment."""
    load_dotenv()
    logger.info("Connecting to Airtable for reports")
    return ReportManager()


@pytest.fixture(scope="module")
def test_subscribers() -> List[dict]:
    """Retrieve subscribers flagged for testing."""
    subs = get_test_subscribers()
    for sub in subs:
        fields = sub.get("fields", {})
        logger.debug(
            "Subscriber %s email=%s Test=%s",
            sub.get("id"),
            fields.get("Email"),
            fields.get("Test"),
        )
    logger.info("Loaded %d test subscriber(s)", len(subs))
    return subs


@pytest.fixture(scope="module")
def approved_reports(manager: ReportManager) -> List[dict]:
    """Get all approved reports for the current month/year."""
    month, year = get_current_month_year()
    reports = manager.get_approved_reports(month, year)
    logger.info("Found %d approved report(s) for %s %s", len(reports), month, year)
    return reports


def _retry(func, retries: int = 3, delay: int = 2):
    """Simple retry helper."""
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Attempt %d/%d failed: %s", attempt, retries, exc, exc_info=True)
            if attempt == retries:
                raise
            time.sleep(delay)


def _send_report(report: dict):
    fields = report["fields"]
    state = fields["State"]
    month = fields["Month"]
    year = fields["Year"]
    report_id = report["id"]
    path = f"reports/{state.lower().replace(' ', '_')}_{month.lower()}_{year}.html"

    if not os.path.exists(path):
        logger.error("Report file not found: %s", path)
        raise EmailSendError(f"Missing report HTML: {path}")

    logger.info("Sending %s to test subscribers", fields.get("Name"))

    return _retry(
        lambda: send_newsletter_embedded_with_subscriber_tracking(
            state=state,
            month=month,
            year=year,
            report_path=path,
            report_record_id=report_id,
            test_mode=True,
        )
    )


def _call_webhook(message_id: str):
    payload = {
        "RecordType": "Delivery",
        "MessageID": message_id,
        "DeliveredAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    url = "https://taking-rate-postmark-webhook.onrender.com/webhook/postmark"

    def _post():
        logger.debug("POST %s %s", url, json.dumps(payload))
        resp = requests.post(url, json=payload, timeout=10)
        logger.info("Webhook response %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
        return resp

    return _retry(_post)


def test_send_approved_reports(approved_reports, test_subscribers):
    """Send approved reports to test subscribers and verify webhook."""
    if not approved_reports:
        pytest.skip("No approved reports to send")
    if not test_subscribers:
        pytest.skip("No test subscribers available")

    errors = []
    for report in approved_reports:
        try:
            responses = _send_report(report)
            for res in responses:
                message_id = res.get("MessageID")
                if message_id:
                    try:
                        _call_webhook(message_id)
                    except Exception as exc:  # pylint: disable=broad-except
                        logger.error("Webhook call failed for %s: %s", message_id, exc, exc_info=True)
                        errors.append(str(exc))
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Failed to process report %s: %s", report.get("id"), exc, exc_info=True)
            errors.append(str(exc))

    if errors:
        logger.error("Test completed with errors: %s", errors)
        pytest.fail("Errors occurred during sending and webhook calls")
    else:
        logger.info("All approved reports processed successfully")
