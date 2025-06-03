"""
Monthly Newsletter Workflow
Run with: python -m src.monthly_workflow generate
     or: python -m src.monthly_workflow send
"""
import os
import sys
import subprocess
from datetime import datetime
from dotenv import load_dotenv

from src.report_manager import ReportManager
from src.email_service import send_newsletter_batch
from src.reports.state_newsletter import generate_state_report  # You'll need to update this import

load_dotenv()

# All US states
ALL_STATES = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
    'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
    'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
    'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
    'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
    'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
    'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
    'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
    'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
    'West Virginia', 'Wisconsin', 'Wyoming'
]

def get_current_month_year():
    """Get current month and year"""
    now = datetime.now()
    return now.strftime('%B'), str(now.year)  # e.g., "March", "2024"

def generate_all_reports(dry_run=False):
    """Generate reports for all states with activity"""
    month, year = get_current_month_year()
    manager = ReportManager()
    
    print(f"=== Generating {month} {year} Reports ===\n")
    
    generated_count = 0
    
    for state in ALL_STATES:
        # Check if report already exists
        existing = manager.get_report_by_state_month_year(state, month, year)
        if existing:
            print(f"⏭️  {state}: Report already exists")
            continue
        
        # TODO: Check if state has activity in your database
        # has_activity = check_state_activity(state, month, year)
        # if not has_activity:
        #     print(f"⏭️  {state}: No activity this month")
        #     continue
        
        try:
            print(f"📄 Generating {state}...", end='', flush=True)
            
            if not dry_run:
                # Generate the report
                # You'll need to update this to match your actual report generation
                output_path = f"docs/reports/{year}-{month.lower()[:3]}/{state.lower().replace(' ', '-')}.html"
                
                # TODO: Call your actual report generator
                # generate_state_report(state, month, year, output_path)
                
                # For now, let's assume you manually place files
                report_url = f"https://jtbrown21.github.io/SERFF-Analytics/reports/{year}-{month.lower()[:3]}/{state.lower().replace(' ', '-')}.html"
                
                # Log to Airtable
                manager.log_report(
                    state=state,
                    month=month,
                    year=year,
                    report_url=report_url
                )
                
                print(" ✓")
                generated_count += 1
            else:
                print(" [DRY RUN]")
                
        except Exception as e:
            print(f" ❌ Error: {e}")
    
    print(f"\n✓ Generated {generated_count} reports")
    
    if generated_count > 0 and not dry_run:
        # Commit and push to GitHub
        print("\n📤 Pushing to GitHub Pages...")
        subprocess.run(['git', 'add', 'docs/reports/'])
        subprocess.run(['git', 'commit', '-m', f'Add {month} {year} reports'])
        subprocess.run(['git', 'push'])
        print("✓ Pushed to GitHub")

def send_approved_reports(dry_run=False):
    """Send all approved reports with full HTML embedded"""
    month, year = get_current_month_year()
    manager = ReportManager()
    
    print(f"=== Sending {month} {year} Approved Reports ===\n")
    
    # Get approved reports
    approved = manager.get_approved_reports(month, year)
    
    if not approved:
        print("❌ No approved reports found")
        return
    
    print(f"Found {len(approved)} approved report(s)\n")
    
    for report in approved:
        fields = report['fields']
        state = fields['State']
        
        print(f"📧 {fields['Name']}:")
        
        # Build local file path
        report_path = f"docs/reports/{year}-{month.lower()[:3]}/{state.lower().replace(' ', '-')}.html"
        
        # Check if file exists
        if not os.path.exists(report_path):
            print(f"  ❌ Report file not found: {report_path}")
            continue
        
        # TODO: Get actual subscribers
        recipients = ['your-email@example.com']  # UPDATE THIS
        
        if not dry_run:
            try:
                # Send the embedded report
                send_newsletter_embedded_batch(
                    state=state,
                    month=fields['Month'],
                    year=fields['Year'],
                    report_path=report_path,  # Local file path, not URL
                    recipients=recipients
                )
                
                # Mark as sent
                manager.mark_as_sent(report['id'])
                print(f"  ✓ Sent embedded report to {len(recipients)} recipients")
                
            except Exception as e:
                print(f"  ❌ Error: {e}")
        else:
            print(f"  [DRY RUN] Would embed and send {report_path}")
def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python -m src.monthly_workflow [generate|send] [--dry-run]")
        return
    
    command = sys.argv[1]
    dry_run = '--dry-run' in sys.argv
    
    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made\n")
    
    if command == 'generate':
        generate_all_reports(dry_run)
    elif command == 'send':
        send_approved_reports(dry_run)
    else:
        print(f"Unknown command: {command}")
        print("Use 'generate' or 'send'")

if __name__ == "__main__":
    main()
