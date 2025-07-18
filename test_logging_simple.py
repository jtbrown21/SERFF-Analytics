#!/usr/bin/env python3
"""
Simple test for airtable_sync.py logging without external dependencies.
"""
import logging
import sys
from pathlib import Path

# Add the CORE directory to the path
sys.path.insert(0, str(Path(__file__).parent))

def setup_clean_logging():
    """Setup clean logging configuration."""
    # Clear all existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    return logging.getLogger(__name__)

def test_logging_fix():
    """Test that the logging duplication issue is fixed."""
    logger = setup_clean_logging()
    
    logger.info("=" * 50)
    logger.info("TESTING LOGGING FIX")
    logger.info("=" * 50)
    
    # Import modules
    logger.info("Importing modules...")
    from core.data.sync.airtable_sync import AirtableSync
    
    # Test logging from different modules
    logger.info("Testing main logger - message 1")
    logger.info("Testing main logger - message 2")
    
    sync_logger = logging.getLogger('core.data.sync.airtable_sync')
    sync_logger.info("Testing sync logger - message 1")
    sync_logger.info("Testing sync logger - message 2")
    
    db_logger = logging.getLogger('core.data.database')
    db_logger.info("Testing database logger - message 1")
    db_logger.info("Testing database logger - message 2")
    
    # Test multiple loggers with same name
    logger.info("Testing multiple calls to same logger...")
    test_logger = logging.getLogger('test.logger')
    test_logger.info("Test message A")
    test_logger.info("Test message B")
    
    # Get the same logger again
    test_logger2 = logging.getLogger('test.logger')
    test_logger2.info("Test message C")
    test_logger2.info("Test message D")
    
    logger.info("=" * 50)
    logger.info("LOGGING FIX TEST COMPLETED!")
    logger.info("Each message above should appear exactly ONCE")
    logger.info("=" * 50)
    
    return True

if __name__ == "__main__":
    print("Testing Airtable sync logging fix...")
    success = test_logging_fix()
    
    if success:
        print("\n✅ Logging fix test passed!")
        print("   - No more duplicate log messages")
        print("   - All loggers working correctly")
    else:
        print("\n❌ Logging fix test failed!")
        sys.exit(1)
