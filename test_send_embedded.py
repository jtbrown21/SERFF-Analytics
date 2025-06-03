from src.email_service import send_newsletter_embedded

# Test with your Nevada report
send_newsletter_embedded(
    state="Nevada",
    month="August", 
    year="2024",
    report_path="reports/nevada_august_2024.html",
    recipients=["jt@thehypogroup.com"]
)
