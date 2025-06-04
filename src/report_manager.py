"""
Report Manager - Handles all Airtable operations for the Reports table
"""
import os
from datetime import datetime
from pyairtable import Table
from dotenv import load_dotenv

load_dotenv()


class ReportManager:
    def __init__(self):
        """Initialize connection to Airtable Reports table"""
        self.table = Table(
            os.getenv('AIRTABLE_API_KEY'),
            os.getenv('AIRTABLE_BASE_ID'),
            'Reports'
        )
    
    def log_report(self, state, month, year, report_url, name=None, filings=None):
        """
        Create a new report entry in Airtable
        
        Args:
            state: State name (e.g., "Illinois")
            month: Month name (e.g., "March" or "03")
            year: Year (e.g., "2024")
            report_url: GitHub Pages URL for the report
            name: Optional name/title for the report
            filings: Optional list of filing record IDs to link
            
        Returns:
            The created Airtable record
        """
        # Build the name if not provided
        if not name:
            name = f"{state} {month} {year}"
        
        record_data = {
            'Name': name,
            'State': state,
            'Month': month,
            'Year': year,
            'Report URL': report_url,
            'Status': 'Generated'
        }
        if filings:
            record_data['Filings'] = filings

        record = self.table.create(record_data)
        
        print(f"✓ Logged {state} {month} {year} report to Airtable")
        return record
    
    def get_reports_by_status(self, status, month=None, year=None):
        """
        Get all reports with a specific status
        
        Args:
            status: "Generated", "Approved", or "Sent"
            month: Optional - filter by specific month
            year: Optional - filter by specific year
            
        Returns:
            List of Airtable records
        """
        formula = f"{{Status}}='{status}'"
        
        if month:
            formula = f"AND({formula}, {{Month}}='{month}')"
        
        if year:
            formula = f"AND({formula}, {{Year}}='{year}')"
        
        return self.table.all(formula=formula)
    
    def get_approved_reports(self, month, year):
        """Get all approved reports for a specific month and year"""
        return self.get_reports_by_status('Approved', month, year)
    
    def approve_report(self, record_id):
        """
        Mark a report as approved
        
        Args:
            record_id: Airtable record ID
        """
        return self.table.update(record_id, {
            'Status': 'Approved'
        })
    
    def mark_as_sent(self, record_id):
        """
        Mark a report as sent
        
        Args:
            record_id: Airtable record ID
        """
        return self.table.update(record_id, {
            'Status': 'Sent'
        })
    
    def get_report_by_state_month_year(self, state, month, year):
        """Get a specific report by state, month, and year"""
        formula = f"AND({{State}}='{state}', {{Month}}='{month}', {{Year}}='{year}')"
        records = self.table.all(formula=formula)
        return records[0] if records else None
    
    def update_name(self, record_id, name):
        """Update the name/title of a report"""
        return self.table.update(record_id, {
            'Name': name
        })
    
    def get_all_reports(self, year=None):
        """Get all reports, optionally filtered by year"""
        if year:
            return self.table.all(formula=f"{{Year}}='{year}'")
        return self.table.all()


# Convenience functions for direct use
def log_report_to_airtable(state, month, year, report_url, name=None):
    """Quick function to log a report"""
    manager = ReportManager()
    return manager.log_report(state, month, year, report_url, name)


def get_approved_reports_for_month(month, year):
    """Quick function to get approved reports"""
    manager = ReportManager()
    return manager.get_approved_reports(month, year)


# Test the module
if __name__ == "__main__":
    print("Testing Report Manager...")
    
    # Initialize manager
    manager = ReportManager()
    
    print("\nYour Airtable fields:")
    print("- Name (text)")
    print("- State (Single Select)")
    print("- Month (Single Select)")
    print("- Year (text)")
    print("- Report URL (URL)")
    print("- Status (Single Select)")
    
    # Test logging a report
    try:
        # Note: Month should match your Single Select options
        # Common formats: "August", "Aug", "08", or "8"
        test_record = manager.log_report(
            state="Nevada",
            month="August",  # Change this to match your Single Select options
            year="2024",
            report_url="https://jtbrown21.github.io/SERFF-Analytics/nevada_august_2024.html",
            name="Nevada August 2024 Insurance Report"
        )
        
        print(f"\n✅ Successfully created record ID: {test_record['id']}")
        print("\nRecord details:")
        for field, value in test_record['fields'].items():
            print(f"  {field}: {value}")
            
    except Exception as e:
        print(f"\n❌ Error creating record: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure 'Generated' is an option in your Status field")
        print("2. Check that State includes 'Nevada' as an option")
        print("3. Check that Month includes 'August' (or '08') as an option")
    
    # Show all reports
    try:
        print("\n\nAll reports in Airtable:")
        reports = manager.get_all_reports()
        if reports:
            for report in reports:
                fields = report['fields']
                state = fields.get('State', 'N/A')
                month = fields.get('Month', 'N/A')
                year = fields.get('Year', 'N/A')
                status = fields.get('Status', 'N/A')
                print(f"- {state} {month} {year}: {status}")
        else:
            print("No reports found.")
    except Exception as e:
        print(f"Error fetching reports: {e}")