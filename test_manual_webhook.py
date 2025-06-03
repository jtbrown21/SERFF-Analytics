import requests
import json
from datetime import datetime

# Replace this with your actual Postmark Message ID from Airtable
MESSAGE_ID = "6f238ab3-b096-4ffb-86f7-8602ba0a6aca"

# First, test if the webhook is running
try:
    test_response = requests.get('http://127.0.0.1:5000/webhook/test')
    print(f"Webhook test endpoint: {test_response.status_code}")
    print(f"Response: {test_response.text}\n")
except requests.exceptions.ConnectionError:
    print("❌ ERROR: Webhook handler is not running!")
    exit(1)

# Test 1: Delivery Event
print("Testing Delivery event...")
delivery_event = {
    "RecordType": "Delivery", 
    "MessageID": MESSAGE_ID,
    "DeliveredAt": datetime.now().isoformat()
}

try:
    response = requests.post(
        'http://127.0.0.1:5000/webhook/postmark',
        json=delivery_event,
        headers={'Content-Type': 'application/json'}
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {response.headers}")
    print(f"Response Text: '{response.text}'")  # This will show what's actually returned
    
    # Only try to parse JSON if we have content
    if response.text:
        try:
            print(f"Response JSON: {response.json()}")
        except:
            print("Response is not valid JSON")
    else:
        print("Response is empty")
        
except Exception as e:
    print(f"Error: {e}")

print("\n✅ Check Terminal 1 (webhook handler) for error messages!")