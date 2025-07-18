#!/usr/bin/env python3
"""
Example script demonstrating the new CORE architecture.

This script shows how to use the new modular system for common tasks.
"""

import sys
from datetime import datetime

# Import the new core modules
from core import settings, DataManager, WorkflowEngine, ReportManager
from core.models import ReportType, RateFiling, FilingStatus, InsuranceType
from core.utils import get_logger

# Set up logging
logger = get_logger(__name__)

def main():
    """Main example function."""
    print("🏗️ CORE Architecture Example")
    print("=" * 50)
    
    # 1. Configuration Management
    print("\n1. Configuration Management")
    print(f"   Environment: {settings.environment}")
    print(f"   Database: {settings.database.duckdb_path}")
    print(f"   Debug mode: {settings.debug}")
    
    # 2. Data Management
    print("\n2. Data Management")
    data_manager = DataManager()
    
    # Create a sample rate filing
    sample_filing = RateFiling(
        filing_id="SAMPLE_001",
        company_name="Sample Insurance Co",
        state="CA",
        filing_type=InsuranceType.AUTO,
        status=FilingStatus.APPROVED,
        rate_change_percent=5.2,
        rate_change_description="Rate increase due to claims experience",
        source="example_script"
    )
    
    print(f"   Created sample filing: {sample_filing.filing_id}")
    print(f"   Company: {sample_filing.company_name}")
    print(f"   State: {sample_filing.state}")
    print(f"   Rate change: {sample_filing.rate_change_percent}%")
    
    # Save the filing
    success = data_manager.save_filing(sample_filing)
    print(f"   Save result: {'Success' if success else 'Failed'}")
    
    # 3. Analytics
    print("\n3. Analytics")
    from core.analytics import AnalyticsEngine
    
    analytics = AnalyticsEngine()
    trends = analytics.calculate_market_trends([sample_filing], state="CA")
    print(f"   Market trends calculated: {trends['trend_direction']}")
    
    # 4. Reporting
    print("\n4. Reporting")
    report_manager = ReportManager()
    
    report_data = report_manager.generate_report(
        report_type=ReportType.AGENT_INTEL,
        filings=[sample_filing],
        state="CA"
    )
    
    print(f"   Generated report: {report_data.report_id}")
    print(f"   Title: {report_data.title}")
    print(f"   Filings included: {report_data.total_filings}")
    
    # 5. Notifications
    print("\n5. Notifications")
    from core.notifications import NotificationService
    from core.models import AgentProfile
    
    notification_service = NotificationService()
    
    # Create a sample agent profile
    agent = AgentProfile(
        agent_id="AGENT_001",
        name="John Doe",
        email="john.doe@example.com",
        state="CA"
    )
    
    # Send a sample notification
    html_content = report_manager.render_html_report(report_data)
    message = notification_service.send_report_email(
        agent_profile=agent,
        report_data=report_data,
        html_content=html_content
    )
    
    print(f"   Email sent to: {message.recipient}")
    print(f"   Subject: {message.subject}")
    print(f"   Status: {message.delivery_status}")
    
    # 6. Workflows
    print("\n6. Workflows")
    workflow_engine = WorkflowEngine()
    
    # Note: This would normally run the full workflow, but we'll just show the interface
    print("   Workflow engine initialized")
    print("   Available workflows:")
    print("   - monthly_report_workflow()")
    print("   - data_sync_workflow()")
    
    # 7. Database Stats
    print("\n7. Database Statistics")
    stats = data_manager.get_stats()
    print(f"   Total filings: {stats['total_filings']}")
    print(f"   Recent updates: {stats['recent_updates']}")
    
    # Clean up
    data_manager.close()
    
    print("\n✅ Example completed successfully!")
    print("\nThe new architecture provides:")
    print("  • Clear module boundaries")
    print("  • Centralized configuration")
    print("  • Consistent data models")
    print("  • Unified logging")
    print("  • Workflow orchestration")
    print("  • Easy testing and maintenance")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Example failed: {e}")
        sys.exit(1)
