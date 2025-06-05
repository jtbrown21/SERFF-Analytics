#!/usr/bin/env python3
"""
Send approved reports to test subscribers via email and verify webhook delivery.
Run with: python send_approved_reports.py
"""

import os
import json
import time
import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import List

import requests
from dotenv import load_dotenv

from src.report_manager import ReportManager
from src.email_service import (
    send_newsletter_embedded_with_subscriber_tracking,
    get_test_subscribers,
)


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

# Also log to console
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def initialize_manager() -> ReportManager:
    """Initialize ReportManager and load environment."""
    load_dotenv()
    logger.info("Connecting to Airtable for reports")
    return ReportManager()


def get_test_subscriber_list() -> List[dict]:
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


def get_approved_reports_list(manager: ReportManager) -> List[dict]:
    """Get all approved reports regardless of month/year."""
    # Try to get all approved reports
    # If your ReportManager requires different parameters, adjust accordingly:
    # Option 1: No parameters (assuming the method can handle this)
    try:
        reports = manager.get_approved_reports()
    except TypeError:
        # Option 2: If it requires month/year, pass None values
        try:
            reports = manager.get_approved_reports(None, None)
        except:
            # Option 3: If there's a separate method for getting all reports
            # reports = manager.get_all_approved_reports()
            # 
            # Option 4: If we must provide values, use wildcards or get multiple months
            # You may need to modify this based on your ReportManager implementation
            logger.error("Unable to get all approved reports. Check ReportManager implementation.")
            raise
    
    logger.info("Found %d approved report(s) total", len(reports))
    return reports


def retry_operation(func, retries: int = 3, delay: int = 2):
    """Simple retry helper."""
    for attempt in range(1, retries + 1):
        try:
            return func()
        except Exception as exc:
            logger.warning("Attempt %d/%d failed: %s", attempt, retries, exc, exc_info=True)
            if attempt == retries:
                raise
            time.sleep(delay)


def get_state_abbreviation(state_name: str) -> str:
    """Convert state name to two-letter abbreviation."""
    state_abbr = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
        "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
        "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
        "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
        "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
        "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
        "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
        "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
        "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
        "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
        "District of Columbia": "DC", "Washington DC": "DC", "Washington D.C.": "DC"
    }
    return state_abbr.get(state_name, state_name.upper()[:2])


def get_month_info(month_name: str) -> tuple:
    """Convert month name to number (MM) and full name."""
    months = {
        "january": ("01", "January"), "february": ("02", "February"), "march": ("03", "March"),
        "april": ("04", "April"), "may": ("05", "May"), "june": ("06", "June"),
        "july": ("07", "July"), "august": ("08", "August"), "september": ("09", "September"),
        "october": ("10", "October"), "november": ("11", "November"), "december": ("12", "December")
    }
    month_lower = month_name.lower()
    if month_lower in months:
        return months[month_lower]
    # Try to handle abbreviated months
    for full_month, (num, full_name) in months.items():
        if full_month.startswith(month_lower[:3]):
            return num, full_name
    # Default fallback
    return "01", "January"


def send_report(report: dict):
    """Send a single report to test subscribers."""
    fields = report["fields"]
    state = fields["State"]
    month = fields["Month"]
    year = fields["Year"]
    report_id = report["id"]
    
    # Convert state to abbreviation
    state_abbr = get_state_abbreviation(state)
    
    # Get month number and full name
    month_num, month_full = get_month_info(month)
    
    # Construct filename: STATE_MM_YYYY.html
    filename = f"{state_abbr}_{month_num}_{year}.html"
    
    # Construct full path: docs/newsletters/monthly/19.0/{state_abbr}/{year}/{month_full}/{filename}
    path = f"docs/newsletters/monthly/19.0/{state_abbr}/{year}/{month_full}/{filename}"

    if not os.path.exists(path):
        logger.error("Report file not found: %s", path)
        raise EmailSendError(f"Missing report HTML: {path}")

    logger.info("Sending %s to test subscribers (file: %s)", fields.get("Name"), filename)

    return retry_operation(
        lambda: send_newsletter_embedded_with_subscriber_tracking(
            state=state,
            month=month,
            year=year,
            report_path=path,
            report_record_id=report_id,
            test_mode=True,
        )
    )

    logger.info("Sending %s to test subscribers", fields.get("Name"))

    return retry_operation(
        lambda: send_newsletter_embedded_with_subscriber_tracking(
            state=state,
            month=month,
            year=year,
            report_path=path,
            report_record_id=report_id,
            test_mode=True,
        )
    )


def call_webhook(message_id: str):
    """Call webhook to notify of message delivery."""
    payload = {
        "RecordType": "Delivery",
        "MessageID": message_id,
        "DeliveredAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    url = "https://taking-rate-postmark-webhook.onrender.com/webhook/postmark"

    def post_webhook():
        logger.debug("POST %s %s", url, json.dumps(payload))
        resp = requests.post(url, json=payload, timeout=10)
        logger.info("Webhook response %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
        return resp

    return retry_operation(post_webhook)


def send_approved_reports():
    """Main function to send approved reports to test subscribers and verify webhook."""
    try:
        # Initialize components
        manager = initialize_manager()
        test_subscribers = get_test_subscriber_list()
        approved_reports = get_approved_reports_list(manager)
        
        # Check if we have data to process
        if not approved_reports:
            logger.warning("No approved reports to send")
            print("No approved reports found.")
            return True
            
        if not test_subscribers:
            logger.warning("No test subscribers available")
            print("No test subscribers found. Please ensure test subscribers are configured.")
            return True
        
        # Process reports
        errors = []
        successful_sends = 0
        
        for i, report in enumerate(approved_reports, 1):
            report_name = report.get("fields", {}).get("Name", "Unknown")
            state_info = report.get("fields", {}).get("State", "Unknown")
            print(f"\nProcessing report {i}/{len(approved_reports)}: {report_name} ({state_info})")
            
            try:
                responses = send_report(report)
                print(f"  ✓ Report sent successfully")
                successful_sends += 1
                
                # Process webhook calls for each response
                for res in responses:
                    message_id = res.get("MessageID")
                    if message_id:
                        try:
                            call_webhook(message_id)
                            print(f"  ✓ Webhook called for message {message_id}")
                        except Exception as exc:
                            logger.error("Webhook call failed for %s: %s", message_id, exc, exc_info=True)
                            errors.append(f"Webhook error for {message_id}: {str(exc)}")
                            print(f"  ✗ Webhook failed for message {message_id}: {exc}")
                            
            except Exception as exc:
                logger.error("Failed to process report %s: %s", report.get("id"), exc, exc_info=True)
                errors.append(f"Report {report_name}: {str(exc)}")
                print(f"  ✗ Failed to send report: {exc}")
        
        # Summary
        print(f"\n{'='*60}")
        print(f"Summary: {successful_sends}/{len(approved_reports)} reports sent successfully")
        
        if errors:
            print(f"\nErrors encountered ({len(errors)}):")
            for error in errors:
                print(f"  - {error}")
            logger.error("Process completed with errors: %s", errors)
            return False
        else:
            print("\nAll reports processed successfully!")
            logger.info("All approved reports processed successfully")
            return True
            
    except Exception as exc:
        logger.error("Fatal error in main process: %s", exc, exc_info=True)
        print(f"\nFatal error: {exc}")
        return False


def main():
    """Entry point for the script."""
    print("Send Approved Reports - Email Newsletter Delivery")
    print("="*60)
    
    success = send_approved_reports()
    
    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()