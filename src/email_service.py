"""
Email Service - Handles all email sending via Postmark
"""
import os
from postmarker.core import PostmarkClient
from dotenv import load_dotenv

load_dotenv()


def send_newsletter(state, month, year, report_url, recipients):
    """
    Send newsletter to a list of recipients
    
    Args:
        state: State name
        month: Month name
        year: Year
        report_url: URL to the report
        recipients: List of email addresses
        
    Returns:
        List of responses from Postmark
    """
    client = PostmarkClient(server_token=os.getenv('POSTMARK_SERVER_TOKEN'))
    
    # Create the email
    subject = f"{state} Insurance Market Update - {month} {year}"
    
    html_body = f"""
    <h2>{state} Insurance Market Update</h2>
    <p>The {month} {year} insurance rate filing report is now available.</p>
    <p><a href="{report_url}" style="background: #0066cc; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block;">View Report</a></p>
    <hr>
    <p style="color: #666; font-size: 12px;">You're receiving this because you're subscribed to {state} insurance updates.</p>
    """
    
    # Send to all recipients
    responses = []
    for email in recipients:
        response = client.emails.send(
            From='jt@thehypogroup.com',  # Update with your verified domain
            To=email,
            Subject=subject,
            HtmlBody=html_body,
            MessageStream='outbound'
        )
        responses.append(response)
        print(f"  ✓ Sent to {email}")
    
    return responses


def send_newsletter_batch(state, month, year, report_url, recipients):
    """
    Send newsletter to multiple recipients in a single batch
    
    Args:
        state: State name
        month: Month name
        year: Year
        report_url: URL to the report
        recipients: List of email addresses
        
    Returns:
        Response from Postmark batch send
    """
    client = PostmarkClient(server_token=os.getenv('POSTMARK_SERVER_TOKEN'))
    
    # Create the email
    subject = f"{state} Insurance Market Update - {month} {year}"
    
    html_body = f"""
    <h2>{state} Insurance Market Update</h2>
    <p>The {month} {year} insurance rate filing report is now available.</p>
    <p><a href="{report_url}" style="background: #0066cc; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block;">View Report</a></p>
    <hr>
    <p style="color: #666; font-size: 12px;">You're receiving this because you're subscribed to {state} insurance updates.</p>
    """
    
    # Create batch messages
    messages = []
    for email in recipients:
        messages.append({
            'From': 'jt@thehypogroup.com',  # Update with your verified domain
            'To': email,
            'Subject': subject,
            'HtmlBody': html_body,
            'MessageStream': 'outbound'
        })
    
    # Send batch (max 500 per batch in Postmark)
    if len(messages) <= 500:
        response = client.emails.send_batch(*messages)
        print(f"  ✓ Sent batch of {len(messages)} emails")
        return response
    else:
        # Split into batches of 500
        responses = []
        for i in range(0, len(messages), 500):
            batch = messages[i:i+500]
            response = client.emails.send_batch(*batch)
            responses.append(response)
            print(f"  ✓ Sent batch of {len(batch)} emails")
        return responses


def test_postmark_connection():
    """Test if Postmark credentials work"""
    token = os.getenv('POSTMARK_SERVER_TOKEN')
    
    if not token:
        print("ERROR: POSTMARK_SERVER_TOKEN not found in .env file")
        print("Make sure your .env file contains: POSTMARK_SERVER_TOKEN=your-token-here")
        return
    
    client = PostmarkClient(server_token=token)
    
    # Send a test email to yourself
    response = client.emails.send(
        From='jt@thehypogroup.com',  # Update with your verified domain
        To='jt@thehypogroup.com',  # Your email
        Subject='SERFF Analytics Test',
        HtmlBody='<h1>Test successful!</h1><p>Postmark is connected.</p>'
    )
    
    print(f"Email sent! Message ID: {response['MessageID']}")
    return response


def get_email_status(message_id):
    """
    Get the delivery status of a sent email
    
    Args:
        message_id: The MessageID returned when sending
        
    Returns:
        Message details from Postmark
    """
    client = PostmarkClient(server_token=os.getenv('POSTMARK_SERVER_TOKEN'))
    return client.messages.get_outbound_message_details(message_id)


if __name__ == "__main__":
    test_postmark_connection()