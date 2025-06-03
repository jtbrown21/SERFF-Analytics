"""
Webhook Handler for Postmark Events
Updates Airtable Emails table based on email events
"""
import os
from flask import Flask, request, jsonify
from pyairtable import Table
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Initialize Airtable
emails_table = Table(
    os.getenv('AIRTABLE_API_KEY'),
    os.getenv('AIRTABLE_BASE_ID'),
    'Emails'
)


@app.route('/webhook/postmark', methods=['POST'])
def handle_postmark_webhook():
    """Handle incoming Postmark webhook events"""
    
    # Postmark sends JSON data
    data = request.json
    
    if not data:
        return jsonify({'error': 'No data received'}), 400
    
    # Postmark can send single events or batches
    events = data if isinstance(data, list) else [data]
    
    processed = 0
    errors = []
    
    for event in events:
        try:
            process_event(event)
            processed += 1
        except Exception as e:
            errors.append(f"Error processing event: {str(e)}")
    
    return jsonify({
        'processed': processed,
        'errors': errors
    }), 200


def process_event(event):
    """Process a single Postmark event"""
    
    # Get the Postmark Message ID
    message_id = event.get('MessageID')
    if not message_id:
        print(f"No MessageID in event: {event}")
        return
    
    # Find the email record in Airtable
    records = emails_table.all(
        formula=f"{{Postmark Message ID}}='{message_id}'"
    )
    
    if not records:
        print(f"No record found for MessageID: {message_id}")
        return
    
    email_record = records[0]
    record_id = email_record['id']
    
    # Handle different event types
    event_type = event.get('RecordType', '').lower()
    
    if event_type == 'delivery':
        # Email was successfully delivered
        emails_table.update(record_id, {
            'Status': 'Delivered',
            'Delivered Date': event.get('DeliveredAt', datetime.now().isoformat())
        })
        print(f"✓ Marked as Delivered: {message_id}")
    
    elif event_type == 'open':
        # Email was opened
        # Only update if this is the first open
        current_status = email_record['fields'].get('Status', '')
        if current_status != 'Opened':
            emails_table.update(record_id, {
                'Status': 'Opened',
                'Opened Date': event.get('ReceivedAt', datetime.now().isoformat()),
                'Open Count': 1
            })
        else:
            # Increment open count
            current_count = email_record['fields'].get('Open Count', 1)
            emails_table.update(record_id, {
                'Open Count': current_count + 1
            })
        print(f"✓ Marked as Opened: {message_id}")
    
    elif event_type == 'bounce':
        # Email bounced
        bounce_type = event.get('Type', 'Unknown')
        bounce_description = event.get('Description', 'No description')
        
        emails_table.update(record_id, {
            'Status': 'Bounced',
            'Bounce Type': bounce_type,
            'Notes': f"Bounce: {bounce_description}"
        })
        print(f"✓ Marked as Bounced: {message_id} ({bounce_type})")
    
    elif event_type == 'spamcomplaint':
        # Recipient marked as spam
        emails_table.update(record_id, {
            'Status': 'Spam Complaint',
            'Notes': 'Recipient marked email as spam'
        })
        print(f"✓ Marked as Spam Complaint: {message_id}")
    
    elif event_type == 'unsubscribe':
        # Recipient unsubscribed
        emails_table.update(record_id, {
            'Status': 'Unsubscribed',
            'Notes': 'Recipient unsubscribed'
        })
        
        # Also update the subscriber record
        update_subscriber_status(email_record, 'unsubscribed')
        print(f"✓ Marked as Unsubscribed: {message_id}")
    
    else:
        print(f"Unknown event type: {event_type}")


def update_subscriber_status(email_record, status):
    """Update the linked subscriber's status"""
    
    # Get linked subscriber ID
    subscriber_ids = email_record['fields'].get('Subscribers', [])
    if not subscriber_ids:
        return
    
    subscribers_table = Table(
        os.getenv('AIRTABLE_API_KEY'),
        os.getenv('AIRTABLE_BASE_ID'),
        'Subscribers'
    )
    
    for subscriber_id in subscriber_ids:
        try:
            if status == 'unsubscribed':
                subscribers_table.update(subscriber_id, {
                    'Active': False,
                    'Unsubscribe Date': datetime.now().isoformat()
                })
            elif status == 'spam_complaint':
                subscribers_table.update(subscriber_id, {
                    'Active': False,
                    'Notes': 'Marked email as spam'
                })
        except Exception as e:
            print(f"Error updating subscriber {subscriber_id}: {e}")


@app.route('/webhook/test', methods=['GET'])
def test_webhook():
    """Test endpoint to verify webhook is running"""
    return jsonify({
        'status': 'ok',
        'message': 'Postmark webhook handler is running'
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)