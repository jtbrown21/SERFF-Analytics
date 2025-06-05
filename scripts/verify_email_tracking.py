from dotenv import load_dotenv
load_dotenv()

from src.email_service import send_newsletter_embedded_with_tracking
from src.report_manager import ReportManager

def test_email_with_tracking():
    """Test sending email and tracking in Airtable"""
    
    # Get the Nevada report from Airtable
    manager = ReportManager()
    nevada_report = manager.get_report_by_state_month_year("Nevada", "August", "2024")
    
    if not nevada_report:
        print("❌ Nevada report not found in Airtable. Create it first.")
        return
    
    print(f"Found report: {nevada_report['fields']['Name']}")
    print(f"Report ID: {nevada_report['id']}")
    
    # Send email with tracking
    recipients = ["jt@thehypogroup.com"]  # Using your email
    
    print(f"\nSending to {len(recipients)} recipient(s) with tracking...")
    
    responses = send_newsletter_embedded_with_tracking(
        state="Nevada",
        month="August",
        year="2024",
        report_path="reports/nevada_august_2024.html",
        recipients=recipients,
        report_record_id=nevada_report['id']  # Links email to report
    )
    
    print(f"\n✅ Complete! Check:")
    print("1. Your email inbox for the embedded report")
    print("2. Airtable 'Emails' table for the tracking record")
    print("3. The email should be linked to the Nevada report")

if __name__ == "__main__":
    test_email_with_tracking()
