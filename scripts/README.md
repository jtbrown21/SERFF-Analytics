# Utility Scripts

This package contains helper scripts for testing the newsletter workflow and email tracking.
Run any of them with Python's `-m` flag, for example:

```bash
python -m scripts.verify_email_tracking
```

Available scripts:
- **verify_email_tracking.py** – send a newsletter with tracking enabled.
- **analyze_postmark_payload.py** – post a captured Postmark webhook payload to a local endpoint.
- **test_webhook_endpoint.py** – hit the hosted webhook endpoint to confirm it works.
- **check_send_approved.py** – send approved reports to test subscribers and verify delivery.
- **test_subscriber_tracking.py** – send newsletters to subscribers flagged for testing.
- **email_workflow_test.py** – walk through the approval workflow step by step.
