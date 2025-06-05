from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime
from src.report_manager import ReportManager
from src.email_service import send_newsletter  # We'll create this next
from pyairtable import Table

def run_workflow():
    """Run the complete workflow from generation to ready-to-send."""
    
    print("=== Testing Complete Newsletter Workflow ===\n")
    
    # 1. Initialize managers
    report_manager = ReportManager()
    
    # 2. Check what reports are already in Airtable
    print("Step 1: Current reports in Airtable")
    reports = report_manager.get_all_reports()
    for report in reports:
        fields = report['fields']
        print(f"  - {fields.get('Name', 'N/A')}: {fields.get('Status', 'N/A')}")
    
    # 3. Find the Nevada report we just created
    print("\nStep 2: Finding Nevada August 2024 report...")
    nevada_report = report_manager.get_report_by_state_month_year("Nevada", "August", "2024")
    
    if nevada_report:
        print(f"  ✓ Found report: {nevada_report['fields']['Name']}")
        print(f"  Status: {nevada_report['fields']['Status']}")
        print(f"  URL: {nevada_report['fields']['Report URL']}")
        
        print("\n⚠️  ACTION REQUIRED:")
        print("  1. Go to Airtable")
        print("  2. Find the Nevada August 2024 report")
        print("  3. Change Status from 'Generated' to 'Approved'")
        print("  4. Then run: python -m scripts.check_send_approved")
    else:
        print("  ❌ Nevada report not found. Run report_manager test first.")

if __name__ == "__main__":
    run_workflow()
