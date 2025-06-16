"""Email sending utilities for SERFF Analytics."""

from __future__ import annotations

import hmac
import hashlib
import os
import re
import time
import warnings
from typing import Dict, List, Optional, Union

from dotenv import load_dotenv
from postmarker.core import PostmarkClient
from pyairtable import Table

load_dotenv()


class EmailConfig:
    """Configuration helper for email settings."""

    EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    def __init__(self) -> None:
        self.from_email = os.getenv("FROM_EMAIL", "jt@thehypogroup.com")
        self.postmark_token = self._require_env("POSTMARK_SERVER_TOKEN")
        self.unsubscribe_secret = self._require_env("UNSUBSCRIBE_SECRET")
        self.airtable_key = self._require_env("AIRTABLE_API_KEY")
        self.airtable_base = self._require_env("AIRTABLE_BASE_ID")

    def _require_env(self, key: str) -> str:
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Missing required environment variable: {key}")
        return value

    def validate_email(self, email: str) -> bool:
        return bool(self.EMAIL_REGEX.match(email))

    def generate_unsubscribe_token(self, subscriber_id: str) -> str:
        return hmac.new(
            self.unsubscribe_secret.encode(), subscriber_id.encode(), hashlib.sha256
        ).hexdigest()


class EmailSendError(Exception):
    pass


class EmailValidationError(EmailSendError):
    pass


class EmailDeliveryError(EmailSendError):
    pass


class EmailSender:
    """Wrapper around Postmark with basic retry logic."""

    def __init__(self, config: EmailConfig) -> None:
        self.config = config
        self.client = PostmarkClient(server_token=config.postmark_token)

    def send_with_retry(self, max_retries: int = 3, **email_params):
        last_error: Exception | None = None
        for attempt in range(max_retries):
            try:
                if not self.config.validate_email(email_params.get("To", "")):
                    raise EmailValidationError(f"Invalid email: {email_params.get('To')}")
                return self.client.emails.send(**email_params)
            except Exception as exc:  # pragma: no cover - network
                last_error = exc
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                continue
        raise EmailDeliveryError(f"Failed after {max_retries} attempts") from last_error


def clean_html_for_email(html_content: str) -> str:
    """Basic HTML cleanup for email compatibility."""
    import re as _re

    html_content = _re.sub(
        r"<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>", "", html_content, flags=_re.IGNORECASE
    )
    html_content = html_content.replace("file://", "https://")
    email_css = (
        "<style>body{margin:0;padding:0;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;}"
        "table{border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt;}"
        "img{border:0;height:auto;line-height:100%;outline:none;text-decoration:none;-ms-interpolation-mode:bicubic;}"
        "</style>"
    )
    if "<head>" in html_content:
        html_content = html_content.replace("<head>", f"<head>\n{email_css}")
    return html_content


def get_test_subscribers() -> List[dict]:
    """Retrieve subscribers flagged for testing."""
    table = Table(EmailConfig().airtable_key, EmailConfig().airtable_base, "Subscribers")
    return table.all(formula="{Test}=TRUE()")


def get_subscribers_by_state(state: str) -> List[dict]:
    """Retrieve active subscribers for a specific state."""
    table = Table(EmailConfig().airtable_key, EmailConfig().airtable_base, "Subscribers")
    return table.all(formula=f"AND(FIND('{state}', {{State}}), {{Active}}=TRUE())")


class Newsletter:
    """Unified newsletter helper."""

    def __init__(self, sender: EmailSender, config: EmailConfig) -> None:
        self.sender = sender
        self.config = config

    def _build_content(self, report_url: Optional[str], report_path: Optional[str]) -> str:
        if report_path:
            with open(report_path, "r", encoding="utf-8") as f:
                html = f.read()
            return clean_html_for_email(html)
        if report_url:
            return (
                f"<p>The latest insurance rate filing report is available.</p>"
                f'<p><a href="{report_url}" '
                'style="background:#0066cc;color:white;padding:10px 20px;text-decoration:none;border-radius:5px;display:inline-block;">View Report</a></p>'
            )
        raise ValueError("Either report_url or report_path must be provided")

    def _add_unsubscribe_footer(self, html_content: str, subscriber_id: str, state: str) -> str:
        token = self.config.generate_unsubscribe_token(subscriber_id)
        footer = (
            '<hr style="margin-top:50px;border:1px solid #eee;">'
            '<div style="text-align:center;padding:20px;font-size:12px;color:#666;">'
            f"<p>You're receiving this because you're subscribed to {state} insurance updates.</p>"
            f'<p><a href="https://taking-rate-postmark-webhook.onrender.com/unsubscribe?id={subscriber_id}&token={token}" '
            'style="color:#666;text-decoration:underline;">Unsubscribe from these emails</a></p>'
            "</div>"
        )
        if "</body>" in html_content:
            return html_content.replace("</body>", f"{footer}</body>")
        return html_content + footer

    def _track_email(self, subscriber: dict, report_record_id: str, message_id: str) -> None:
        try:
            table = Table(self.config.airtable_key, self.config.airtable_base, "Emails")
            table.create(
                {
                    "Subscribers": [subscriber.get("id")],
                    "Report": [report_record_id],
                    "Postmark Message ID": message_id,
                }
            )
        except Exception as exc:  # pragma: no cover - external service
            print(f"⚠️ Failed to log {subscriber.get('fields', {}).get('Email')} to Airtable: {exc}")

    def send(
        self,
        state: str,
        month: str,
        year: int,
        recipients: Union[List[str], List[Dict]],
        report_url: Optional[str] = None,
        report_path: Optional[str] = None,
        report_record_id: Optional[str] = None,
        test_mode: bool = False,
        track_in_airtable: bool = True,
    ) -> Dict:
        if test_mode:
            recipients = get_test_subscribers()
        elif recipients and isinstance(recipients[0], str):
            recipients = [{"fields": {"Email": email}} for email in recipients]

        subject = f"{state} Insurance Market Update - {month} {year}"
        html_body = self._build_content(report_url, report_path)

        results = {"sent": [], "failed": [], "invalid": []}
        for recipient in recipients:
            email = recipient.get("fields", {}).get("Email")
            if not email:
                continue
            if not self.config.validate_email(email):
                results["invalid"].append(email)
                continue
            try:
                personalized_html = self._add_unsubscribe_footer(
                    html_body, recipient.get("id", ""), state
                )
                response = self.sender.send_with_retry(
                    From=self.config.from_email,
                    To=email,
                    Subject=subject,
                    HtmlBody=personalized_html,
                    MessageStream="broadcast",
                )
                if track_in_airtable and report_record_id:
                    self._track_email(recipient, report_record_id, response["MessageID"])
                results["sent"].append(email)
            except Exception as exc:
                results["failed"].append({"email": email, "error": str(exc)})
        return results


# Utility functions for status tracking


def test_postmark_connection():
    token = os.getenv("POSTMARK_SERVER_TOKEN")
    if not token:
        print("ERROR: POSTMARK_SERVER_TOKEN not found in .env file")
        return
    client = PostmarkClient(server_token=token)
    response = client.emails.send(
        From="jt@thehypogroup.com",
        To="jt@thehypogroup.com",
        Subject="SERFF Analytics Test",
        HtmlBody="<h1>Test successful!</h1><p>Postmark is connected.</p>",
    )
    print(f"Email sent! Message ID: {response['MessageID']}")
    return response


def get_email_status(message_id: str):
    client = PostmarkClient(server_token=os.getenv("POSTMARK_SERVER_TOKEN"))
    return client.messages.get_outbound_message_details(message_id)


def mark_email_opened(postmark_message_id: str) -> bool:
    table = Table(EmailConfig().airtable_key, EmailConfig().airtable_base, "Emails")
    records = table.all(formula=f"{{Postmark Message ID}}='{postmark_message_id}'")
    if records:
        table.update(
            records[0]["id"],
            {"Status": "Opened", "Opened Date": datetime.now().isoformat()},
        )
        return True
    return False


# Temporary compatibility wrapper


def send_newsletter_embedded_with_subscriber_tracking(
    state: str,
    month: str,
    year: int,
    report_path: str,
    report_record_id: str,
    test_mode: bool = True,
):
    warnings.warn("Deprecated: Use Newsletter.send()", DeprecationWarning)
    config = EmailConfig()
    sender = EmailSender(config)
    newsletter = Newsletter(sender, config)
    return newsletter.send(
        state=state,
        month=month,
        year=year,
        recipients=[],
        report_path=report_path,
        report_record_id=report_record_id,
        test_mode=test_mode,
    )


if __name__ == "__main__":
    test_postmark_connection()
