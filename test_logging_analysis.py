#!/usr/bin/env python3
"""
Simple test to identify duplicate logging handlers.
"""
import logging
import sys
from pathlib import Path

# Add the CORE directory to the path
sys.path.insert(0, str(Path(__file__).parent))

def analyze_logging_setup():
    """Analyze current logging setup to identify duplicates."""
    print("=== LOGGING ANALYSIS ===")
    
    # Check root logger
    root_logger = logging.getLogger()
    print(f"Root logger handlers: {len(root_logger.handlers)}")
    for i, handler in enumerate(root_logger.handlers):
        print(f"  Handler {i}: {type(handler).__name__} - {handler}")
    
    # Check if handlers have propagate set
    print(f"Root logger propagate: {root_logger.propagate}")
    
    # Import modules that might set up logging
    print("\n=== IMPORTING MODULES ===")
    
    print("Importing core.config...")
    from core.config import settings
    
    print("Importing core.data.database...")
    from core.data.database import DatabaseManager
    
    print("Importing core.data.sync.airtable_sync...")
    from core.data.sync.airtable_sync import AirtableSync
    
    print("\n=== CHECKING LOGGERS AFTER IMPORTS ===")
    
    # Check specific loggers
    sync_logger = logging.getLogger('core.data.sync.airtable_sync')
    db_logger = logging.getLogger('core.data.database')
    
    print(f"Sync logger handlers: {len(sync_logger.handlers)}")
    for i, handler in enumerate(sync_logger.handlers):
        print(f"  Handler {i}: {type(handler).__name__} - {handler}")
    print(f"Sync logger propagate: {sync_logger.propagate}")
    
    print(f"DB logger handlers: {len(db_logger.handlers)}")
    for i, handler in enumerate(db_logger.handlers):
        print(f"  Handler {i}: {type(handler).__name__} - {handler}")
    print(f"DB logger propagate: {db_logger.propagate}")
    
    # Check root logger again
    print(f"Root logger handlers after imports: {len(root_logger.handlers)}")
    for i, handler in enumerate(root_logger.handlers):
        print(f"  Handler {i}: {type(handler).__name__} - {handler}")
    
    print("\n=== TESTING LOG MESSAGES ===")
    
    # Set up basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True  # This should clear existing handlers
    )
    
    print("After basicConfig:")
    print(f"Root logger handlers: {len(root_logger.handlers)}")
    
    # Test messages
    print("\nTesting log messages:")
    sync_logger.info("Test message from sync logger")
    db_logger.info("Test message from db logger")
    
    return True

if __name__ == "__main__":
    analyze_logging_setup()
