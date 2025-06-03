"""
Email Service - Handles all email sending via Postmark
"""
import os
from postmarker.core import PostmarkClient
from dotenv import load_dotenv
from pyairtable import Table
from datetime import datetime

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


def send_newsletter_embedded(state, month, year, report_path, recipients):
    """
    Send newsletter with full HTML report embedded in email body
    
    Args:
        state: State name
        month: Month name
        year: Year
        report_path: Local path to HTML report file
        recipients: List of email addresses
        
    Returns:
        List of responses from Postmark
    """
    client = PostmarkClient(server_token=os.getenv('POSTMARK_SERVER_TOKEN'))
    
    # Read the full HTML report
    with open(report_path, 'r', encoding='utf-8') as f:
        report_html = f.read()
    
    # Clean HTML for email
    report_html = clean_html_for_email(report_html)
    
    # Create subject line
    subject = f"{state} Insurance Market Update - {month} {year}"
    
    # Send to all recipients
    responses = []
    for email in recipients:
        response = client.emails.send(
            From='jt@thehypogroup.com',  # Update with your verified domain
            To=email,
            Subject=subject,
            HtmlBody=report_html,  # Full report as email body
            MessageStream='outbound'
        )
        responses.append(response)
        print(f"  ✓ Sent embedded report to {email}")
    
    return responses


def get_test_subscribers():
    """Get all test subscribers from Airtable"""
    subscribers_table = Table(
        os.getenv('AIRTABLE_API_KEY'),
        os.getenv('AIRTABLE_BASE_ID'),
        'Subscribers'
    )
    
    # Get subscribers where Test = TRUE
    test_subscribers = subscribers_table.all(formula="{Test}=TRUE()")
    
    return test_subscribers


def get_subscribers_by_state(state):
    """Get all active subscribers for a specific state"""
    subscribers_table = Table(
        os.getenv('AIRTABLE_API_KEY'),
        os.getenv('AIRTABLE_BASE_ID'),
        'Subscribers'
    )
    
    # Get subscribers for this state (assuming State is a multiple select or single select)
    # Adjust formula based on your field type
    state_subscribers = subscribers_table.all(
        formula=f"AND(FIND('{state}', {{State}}), {{Active}}=TRUE())"
    )
    
    return state_subscribers

# Add this NEW function
def add_unsubscribe_footer(html_content, subscriber_id, state):
    """Add unsubscribe link to email footer"""
    
    import hashlib
    token = hashlib.md5(f"{subscriber_id}-{os.getenv('UNSUBSCRIBE_SECRET', 'default-secret')}".encode()).hexdigest()
    
    unsubscribe_footer = f"""
    <hr style="margin-top: 50px; border: 1px solid #eee;">
    <div style="text-align: center; padding: 20px; font-size: 12px; color: #666;">
        <p>You're receiving this because you're subscribed to {state} insurance updates.</p>
        <p>
            <a href="https://taking-rate-postmark-webhook.onrender.com/unsubscribe?id={subscriber_id}&token={token}" 
               style="color: #666; text-decoration: underline;">
                Unsubscribe from these emails
            </a>
        </p>
    </div>
    """
    
    if '</body>' in html_content:
        html_content = html_content.replace('</body>', f'{unsubscribe_footer}</body>')
    else:
        html_content += unsubscribe_footer
    
    return html_content


def send_newsletter_embedded_with_subscriber_tracking(state, month, year, report_path, report_record_id, test_mode=True):
    """
    Send embedded newsletter to subscribers and track in Airtable
    
    Args:
        state: State name
        month: Month name
        year: Year
        report_path: Local path to HTML report file
        report_record_id: Airtable record ID of the report
        test_mode: If True, only send to test subscribers
        
    Returns:
        List of responses from Postmark
    """
    client = PostmarkClient(server_token=os.getenv('POSTMARK_SERVER_TOKEN'))
    emails_table = Table(
        os.getenv('AIRTABLE_API_KEY'),
        os.getenv('AIRTABLE_BASE_ID'),
        'Emails'
    )
    
    # Get subscribers
    if test_mode:
        print("📧 Test Mode: Sending to test subscribers only")
        subscribers = get_test_subscribers()
    else:
        print(f"📧 Production Mode: Sending to {state} subscribers")
        subscribers = get_subscribers_by_state(state)
    
    if not subscribers:
        print("❌ No subscribers found")
        return []
    
    print(f"Found {len(subscribers)} subscriber(s)")
    
    # Read the full HTML report
    with open(report_path, 'r', encoding='utf-8') as f:
        report_html = f.read()
    
    # Clean HTML for email
    report_html = clean_html_for_email(report_html)
    
    subject = f"{state} Insurance Market Update - {month} {year}"
    
    # Send to all subscribers and track
    responses = []
    for subscriber in subscribers:
        subscriber_id = subscriber['id']
        subscriber_email = subscriber['fields'].get('Email')
        subscriber_name = subscriber['fields'].get('Name', 'Subscriber')

        if not subscriber_email:
            print(f"  ⚠️  No email for subscriber: {subscriber_name}")
            continue

        try:
            # Add unsubscribe footer to this subscriber's email
            personalized_html = add_unsubscribe_footer(report_html, subscriber_id, state)
            
            # Generate token for List-Unsubscribe header (same as in footer)
            import hashlib
            token = hashlib.md5(f"{subscriber_id}-{os.getenv('UNSUBSCRIBE_SECRET', 'default-secret')}".encode()).hexdigest()
            
            # Send via Postmark
            response = client.emails.send(
                From='jt@thehypogroup.com',
                To=subscriber_email,
                Subject=subject,
                HtmlBody=personalized_html,  # <-- NOW USES PERSONALIZED VERSION
                MessageStream='outbound',
               Headers={
                        "List-Unsubscribe": f"<https://taking-rate-postmark-webhook.onrender.com/unsubscribe?id={subscriber_id}&token={token}>"
                }
            )
            
            # Log to Emails table with linked subscriber record
            email_record = emails_table.create({
                'Subscribers': [subscriber_id],  # Link to subscriber record
                'Report': [report_record_id],     # Link to report record
                'Subject Line': subject,
                'Postmark Message ID': response['MessageID'],
                'Status': 'Sent',
                'Email Type': 'Test'
            })
            
            responses.append(response)
            print(f"  ✓ Sent to {subscriber_name} ({subscriber_email}) - Tracked in Airtable")
            
        except Exception as e:
            print(f"  ❌ Failed to send to {subscriber_name} ({subscriber_email}): {e}")
            
            # Log failure
            try:
                emails_table.create({
                    'Subscribers': [subscriber_id],
                    'Report': [report_record_id],
                    'Subject Line': subject,
                    'Status': 'Failed',
                    'Email Type': 'Test',
                    'Notes': str(e)
                })
            except:
                print(f"     Also failed to log to Airtable")
    
    return responses


def send_newsletter_embedded_batch_with_tracking(state, month, year, report_path, recipients, report_record_id):
    """
    Send embedded newsletter in batch and track each email in Airtable
    
    Args:
        state: State name
        month: Month name
        year: Year
        report_path: Local path to HTML report file
        recipients: List of email addresses
        report_record_id: Airtable record ID of the report being sent
        
    Returns:
        Response from Postmark batch send
    """
    client = PostmarkClient(server_token=os.getenv('POSTMARK_SERVER_TOKEN'))
    emails_table = Table(
        os.getenv('AIRTABLE_API_KEY'),
        os.getenv('AIRTABLE_BASE_ID'),
        'Emails'
    )
    
    # Read the full HTML report
    with open(report_path, 'r', encoding='utf-8') as f:
        report_html = f.read()
    
    # Clean HTML for email
    report_html = clean_html_for_email(report_html)
    
    subject = f"{state} Insurance Market Update - {month} {year}"
    
    # Create batch messages
    messages = []
    for email in recipients:
        messages.append({
            'From': 'jt@thehypogroup.com',
            'To': email,
            'Subject': subject,
            'HtmlBody': report_html,
            'MessageStream': 'outbound'
        })
    
    # Send batch
    try:
        if len(messages) <= 500:
            response = client.emails.send_batch(*messages)
            print(f"  ✓ Sent batch of {len(messages)} emails")
            
            # Log each email to Airtable
            for i, email in enumerate(recipients):
                try:
                    # Extract MessageID from batch response
                    message_id = response[i]['MessageID'] if isinstance(response, list) else response.get('MessageID', '')
                    
                    emails_table.create({
                        'Subscriber Email': email,
                        'Report': [report_record_id],
                        'Subject': subject,
                        'Sent Date': datetime.now().isoformat(),
                        'Postmark Message ID': message_id,
                        'Status': 'Sent',
                        'Email Type': 'Test'
                    })
                except Exception as e:
                    print(f"  ⚠️  Failed to log {email} to Airtable: {e}")
            
            return response
        else:
            # Handle large batches
            responses = []
            for i in range(0, len(messages), 500):
                batch = messages[i:i+500]
                batch_recipients = recipients[i:i+500]
                response = client.emails.send_batch(*batch)
                responses.append(response)
                print(f"  ✓ Sent batch of {len(batch)} emails")
                
                # Log this batch to Airtable
                for j, email in enumerate(batch_recipients):
                    try:
                        message_id = response[j]['MessageID'] if isinstance(response, list) else ''
                        emails_table.create({
                            'Subscriber Email': email,
                            'Report': [report_record_id],
                            'Subject': subject,
                            'Sent Date': datetime.now().isoformat(),
                            'Postmark Message ID': message_id,
                            'Status': 'Sent',
                            'Email Type': 'Test'
                        })
                    except Exception as e:
                        print(f"  ⚠️  Failed to log {email} to Airtable: {e}")
            
            return responses
            
    except Exception as e:
        print(f"  ❌ Batch send failed: {e}")
        
        # Log all as failed
        for email in recipients:
            try:
                emails_table.create({
                    'Subscriber Email': email,
                    'Report': [report_record_id],
                    'Subject': subject,
                    'Sent Date': datetime.now().isoformat(),
                    'Status': 'Failed',
                    'Email Type': 'Test',
                    'Notes': f"Batch send error: {str(e)}"
                })
            except:
                pass
        
        raise


def clean_html_for_email(html_content):
    """
    Clean HTML content for email compatibility
    """
    import re
    
    # Remove script tags
    html_content = re.sub(r'<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>', '', html_content, flags=re.IGNORECASE)
    
    # Remove any file:// references
    html_content = html_content.replace('file://', 'https://')
    
    # Add email-specific CSS reset if needed
    email_css = """
    <style>
        /* Email CSS Reset */
        body { margin: 0; padding: 0; -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
        table { border-collapse: collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; }
        img { border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; -ms-interpolation-mode: bicubic; }
    </style>
    """
    
    # Insert after <head> tag
    if '<head>' in html_content:
        html_content = html_content.replace('<head>', f'<head>\n{email_css}')
    
    return html_content


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


def mark_email_opened(postmark_message_id):
    """
    Update Airtable when an email is opened (called by webhook)
    
    Args:
        postmark_message_id: The Postmark Message ID
    """
    emails_table = Table(
        os.getenv('AIRTABLE_API_KEY'),
        os.getenv('AIRTABLE_BASE_ID'),
        'Emails'
    )
    
    # Find the email record
    records = emails_table.all(
        formula=f"{{Postmark Message ID}}='{postmark_message_id}'"
    )
    
    if records:
        email_record = records[0]
        emails_table.update(email_record['id'], {
            'Status': 'Opened',
            'Opened Date': datetime.now().isoformat()
        })
        return True
    return False


if __name__ == "__main__":
    test_postmark_connection()