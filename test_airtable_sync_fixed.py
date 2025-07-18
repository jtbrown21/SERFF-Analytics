#!/usr/bin/env python3
"""
Fixed test script for airtable_sync.py to debug duplicate logging issues.
"""
import logging
import sys
from pathlib import Path

# Add the CORE directory to the path
sys.path.insert(0, str(Path(__file__).parent))

def setup_clean_logging():
    """Setup clean logging configuration without duplicates."""
    
    # Get the root logger and clear ALL handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Also clear handlers from any existing loggers
    for logger_name in list(logging.Logger.manager.loggerDict.keys()):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True  # Ensure propagation to root
    
    # Configure basic logging once
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # Set specific logger levels to reduce noise
    logging.getLogger('pyairtable').setLevel(logging.WARNING)
    logging.getLogger('tenacity').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

def test_simple_airtable_sync():
    """Test the airtable sync functionality with clean logging."""
    logger = setup_clean_logging()
    
    logger.info("Starting clean Airtable sync test...")
    
    try:
        # Import after setting up logging
        from core.data.sync.airtable_sync import AirtableSync
        
        # Initialize the sync
        sync = AirtableSync()
        
        # Test a simple diagnostic
        logger.info("Running a simple diagnostic test...")
        
        # Test field debugging with a small limit
        sync.debug_field_values("Overall Rate Change Number", limit=5)
        
        # Test parsing a few records
        logger.info("Testing record parsing...")
        valid_count, issues = sync.diagnose_rate_change_field(sample_size=3)
        
        logger.info(f"Diagnostic completed: {valid_count} valid records, {len(issues)} issues")
        
        # Test basic sync functionality without hitting Airtable too hard
        logger.info("Testing sync functionality (dry run)...")
        
        # Just test the sync process without actually calling Airtable
        # by testing the methods individually
        
        # Test date parsing
        test_date = "2024-01-15"
        parsed_date = sync._parse_date(test_date)
        logger.info(f"Date parsing test: {test_date} -> {parsed_date}")
        
        # Test number parsing
        test_number = "0.15"
        parsed_number = sync._parse_number(test_number, "test field")
        logger.info(f"Number parsing test: {test_number} -> {parsed_number}")
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False
    
    logger.info("Simple Airtable sync test completed successfully!")
    return True

def test_logging_only():
    """Test just the logging setup without hitting Airtable."""
    logger = setup_clean_logging()
    
    logger.info("Testing logging setup...")
    
    # Import modules and test logging
    from core.data.sync.airtable_sync import AirtableSync
    from core.data.database import DatabaseManager
    
    # Test some log messages
    logger.info("Test message 1")
    sync_logger = logging.getLogger('core.data.sync.airtable_sync')
    sync_logger.info("Test message 2")
    
    db_logger = logging.getLogger('core.data.database')
    db_logger.info("Test message 3")
    
    logger.info("Logging test completed!")
    return True

if __name__ == "__main__":
    print("Starting fixed Airtable sync test...")
    
    # Test 1: Just logging
    print("\n1. Testing logging setup...")
    test_logging_only()
    
    # Test 2: Simple sync test
    print("\n2. Testing simple sync operations...")
    success = test_simple_airtable_sync()
    
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Tests failed!")
        sys.exit(1)
