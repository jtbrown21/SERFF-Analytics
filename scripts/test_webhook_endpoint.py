# test_render_webhook.py
import requests
import json

real_payload = {
    "MessageID": "7c260398-5804-4a28-a2c5-bbe0ee13e04e",
    "Recipient": "jt@thehypogroup.com",
    "DeliveredAt": "2025-06-03T18:16:38Z",
    "Details": "smtp;250 2.0.0 OK...",
    "Tag": "",
    "ServerID": 15926949,
    "Metadata": {},
    "RecordType": "Delivery",
    "MessageStream": "outbound"
}

def main():
    """Send the sample payload to the hosted webhook."""
    response = requests.post(
        'https://taking-rate-postmark-webhook.onrender.com/webhook/postmark',
        json=real_payload,
        headers={'Content-Type': 'application/json'}
    )

    print(f"Response: {response.status_code}")
    print(f"Response text: {response.text}")


if __name__ == "__main__":
    main()
