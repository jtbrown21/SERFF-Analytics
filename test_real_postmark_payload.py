import requests
import json

# This is the EXACT payload from Postmark
real_payload = {
    "MessageID": "7c260398-5804-4a28-a2c5-bbe0ee13e04e",
    "Recipient": "jt@thehypogroup.com",
    "DeliveredAt": "2025-06-03T18:16:38Z",
    "Details": "smtp;250 2.0.0 OK 1748974598 d75a77b69052e-4a435a272d9si142506561cf.392 - gsmtp",
    "Tag": "",
    "ServerID": 15926949,
    "Metadata": {},
    "RecordType": "Delivery",
    "MessageStream": "outbound"
}

# Test against your local webhook
response = requests.post(
    'http://localhost:5000/webhook/postmark',
    json=real_payload,
    headers={'Content-Type': 'application/json'}
)

print(f"Response: {response.status_code}")
print(f"Response text: {response.text}")
