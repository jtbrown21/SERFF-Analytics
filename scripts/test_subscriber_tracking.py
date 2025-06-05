# test_subscriber_tracking.py
from dotenv import load_dotenv
load_dotenv()

from src.email_service import send_newsletter_embedded_with_subscriber_tracking
from src.report_manager import ReportManager

def run_subscriber_email_tracking():
    """Send emails to test subscribers with tracking enabled."""
    
    # Get the Nevada report from Airtable
    manager = ReportManager()
    nevada_report = manager.get_report_by_state_month_year("Nevada", "August", "2024")
    
    if not nevada_report:
        print("❌ Nevada report not found in Airtable.")
        return
    
    print(f"Found report: {nevada_report['fields']['Name']}")
    print(f"Report ID: {nevada_report['id']}")
    
    # Send to test subscribers only
    print("\nSending to test subscribers...")
    
    responses = send_newsletter_embedded_with_subscriber_tracking(
        state="Nevada",
        month="August",
        year="2024",
        report_path="reports/nevada_august_2024.html",
        report_record_id=nevada_report['id'],
        test_mode=True  # Only send to subscribers where Test = TRUE
    )
    
    print(f"\n✅ Complete! Check:")
    print("1. Email inboxes for test subscribers")
    print("2. Airtable 'Emails' table - records should be linked to Subscribers")
    print("3. Each email should show the subscriber name in the linked field")

if __name__ == "__main__":
    run_subscriber_email_tracking()
