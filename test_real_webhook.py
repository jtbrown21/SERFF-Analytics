from src.email_service import send_newsletter_embedded_with_subscriber_tracking
from src.report_manager import ReportManager

# Get Nevada report
manager = ReportManager()
report = manager.get_report_by_state_month_year("Nevada", "August", "2024")

print("Sending test email to trigger real webhooks...")

# Send to test subscribers
responses = send_newsletter_embedded_with_subscriber_tracking(
    state="Nevada",
    month="August", 
    year="2024",
    report_path="reports/nevada_august_2024.html",
    report_record_id=report['id'],
    test_mode=True
)

print("\n✅ Email sent! Now:")
print("1. Watch Terminal 1 for webhook events")
print("2. Open the email to trigger 'Open' event")
print("3. Check Airtable for automatic status updates")
