from dotenv import load_dotenv
load_dotenv()

from src.report_manager import ReportManager
from src.email_service import send_newsletter

def test_send_approved():
    """Test sending approved reports"""
    
    manager = ReportManager()
    
    # Get approved reports
    approved = manager.get_reports_by_status('Approved')
    
    if not approved:
        print("❌ No approved reports found. Please approve a report in Airtable first.")
        return
    
    print(f"Found {len(approved)} approved report(s)\n")
    
    for report in approved:
        fields = report['fields']
        state = fields['State']
        month = fields['Month'] 
        year = fields['Year']
        url = fields['Report URL']
        
        print(f"Would send: {fields['Name']}")
        print(f"URL: {url}")
        
        # For testing, send to yourself
        test_recipients = ['jt@thehypogroup.com']  # UPDATE THIS
        
        print(f"\nSending to {len(test_recipients)} test recipient(s)...")
        
        # Send the email
        responses = send_newsletter(state, month, year, url, test_recipients)
        
        # Mark as sent
        manager.mark_as_sent(report['id'])
        print(f"✓ Marked as sent in Airtable\n")

if __name__ == "__main__":
    test_send_approved()
