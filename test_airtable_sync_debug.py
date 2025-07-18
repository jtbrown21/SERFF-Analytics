#!/usr/bin/env python3
"""
Test script for airtable_sync.py to debug duplicate logging issues.
"""
import logging
import sys
from pathlib import Path

# Add the CORE directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from core.data.sync.airtable_sync import AirtableSync
from core.config import settings

def setup_logging():
    """Setup logging with proper configuration to avoid duplicates."""
    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set specific logger levels
    logging.getLogger('core.data.sync.airtable_sync').setLevel(logging.INFO)
    logging.getLogger('pyairtable').setLevel(logging.WARNING)
    logging.getLogger('tenacity').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

def test_airtable_sync():
    """Test the airtable sync functionality."""
    logger = setup_logging()
    
    logger.info("Starting Airtable sync test...")
    
    try:
        # Initialize the sync
        sync = AirtableSync()
        
        # Test 1: Run diagnostics
        logger.info("=" * 50)
        logger.info("Running diagnostic tests...")
        logger.info("=" * 50)
        
        # Diagnose rate change field
        valid_count, issues = sync.diagnose_rate_change_field(sample_size=5)
        logger.info(f"Rate change field diagnostic: {valid_count} valid records, {len(issues)} issues")
        
        # Test 2: Debug field values
        logger.info("=" * 50)
        logger.info("Debugging field values...")
        logger.info("=" * 50)
        
        sync.debug_field_values("Overall Rate Change Number", limit=10)
        
        # Test 3: Small sync test
        logger.info("=" * 50)
        logger.info("Running small sync test...")
        logger.info("=" * 50)
        
        # Run a manual sync to test the process
        from datetime import datetime, timedelta
        
        # Sync only recent records to limit the test
        since = datetime.now() - timedelta(days=30)
        result = sync.sync_data(since=since)
        
        logger.info(f"Sync completed with result: {result}")
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False
    
    logger.info("Airtable sync test completed successfully!")
    return True

def test_logging_behavior():
    """Test to identify duplicate logging issues."""
    logger = logging.getLogger(__name__)
    
    logger.info("Testing logging behavior...")
    
    # Check current loggers
    logger.info("Current loggers:")
    for name in sorted(logging.Logger.manager.loggerDict.keys()):
        lgr = logging.getLogger(name)
        if lgr.handlers:
            logger.info(f"  {name}: {len(lgr.handlers)} handlers")
            for i, handler in enumerate(lgr.handlers):
                logger.info(f"    Handler {i}: {type(handler).__name__}")
    
    # Check root logger
    root_logger = logging.getLogger()
    logger.info(f"Root logger handlers: {len(root_logger.handlers)}")
    for i, handler in enumerate(root_logger.handlers):
        logger.info(f"  Handler {i}: {type(handler).__name__}")
    
    # Test the airtable sync logger specifically
    sync_logger = logging.getLogger('core.data.sync.airtable_sync')
    logger.info(f"Airtable sync logger handlers: {len(sync_logger.handlers)}")
    for i, handler in enumerate(sync_logger.handlers):
        logger.info(f"  Handler {i}: {type(handler).__name__}")
    
    # Test a few log messages to see if they duplicate
    logger.info("Testing log message 1")
    sync_logger.info("Testing sync logger message 1")
    
    return True

if __name__ == "__main__":
    print("Starting Airtable sync debug test...")
    
    # First test logging behavior
    print("\n1. Testing logging behavior...")
    test_logging_behavior()
    
    # Then test the actual sync
    print("\n2. Testing Airtable sync...")
    success = test_airtable_sync()
    
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Tests failed!")
        sys.exit(1)
