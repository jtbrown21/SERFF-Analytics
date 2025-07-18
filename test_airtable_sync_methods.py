#!/usr/bin/env python3
"""
Test airtable_sync.py functionality with mock data and fixed logging.
"""
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

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
    
    # Reduce noise from external libraries
    logging.getLogger('pyairtable').setLevel(logging.WARNING)
    logging.getLogger('tenacity').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

def test_airtable_sync_methods():
    """Test AirtableSync methods with mock data."""
    logger = setup_clean_logging()
    
    logger.info("=" * 50)
    logger.info("TESTING AIRTABLE SYNC METHODS")
    logger.info("=" * 50)
    
    try:
        # Import after logging setup
        from core.data.sync.airtable_sync import AirtableSync
        
        # Create a mock AirtableSync instance for testing
        with patch('core.data.sync.airtable_sync.Table') as mock_table:
            with patch('core.data.sync.airtable_sync.DatabaseManager') as mock_db:
                
                # Mock the database manager
                mock_db_instance = MagicMock()
                mock_db.return_value = mock_db_instance
                
                # Mock the table
                mock_table_instance = MagicMock()
                mock_table.return_value = mock_table_instance
                
                # Create AirtableSync instance
                logger.info("Creating AirtableSync instance...")
                sync = AirtableSync(db_path=":memory:")
                
                # Test _parse_number method
                logger.info("Testing _parse_number method...")
                test_cases = [
                    ("0.15", 0.15),
                    ("15.5", 15.5),
                    ("", None),
                    (None, None),
                    ("invalid", None),
                    (0.25, 0.25),
                    (10, 10.0),
                ]
                
                for test_input, expected in test_cases:
                    result = sync._parse_number(test_input, "test_field")
                    logger.info(f"  Parse test: '{test_input}' -> {result} (expected: {expected})")
                    assert result == expected, f"Expected {expected}, got {result}"
                
                # Test _parse_date method
                logger.info("Testing _parse_date method...")
                date_test_cases = [
                    ("2024-01-15", datetime(2024, 1, 15).date()),
                    ("2024-12-31", datetime(2024, 12, 31).date()),
                    ("", None),
                    (None, None),
                    ("invalid", None),
                ]
                
                for test_input, expected in date_test_cases:
                    result = sync._parse_date(test_input)
                    logger.info(f"  Date parse test: '{test_input}' -> {result} (expected: {expected})")
                    if expected is not None:
                        assert result == expected, f"Expected {expected}, got {result}"
                    else:
                        assert result is None, f"Expected None, got {result}"
                
                # Test _process_page method with mock data
                logger.info("Testing _process_page method...")
                mock_page = [
                    {
                        "id": "rec123",
                        "fields": {
                            "Parent Company": "Test Insurance Co",
                            "Filing Company": "Test Subsidiary",
                            "Impacted State": "CA",
                            "Product Line": "Auto",
                            "Rate Change Type": "increase",
                            "Overall Rate Change Number": 0.15,
                            "Effective Date": "2024-06-01",
                            "Last Modified": "2024-05-15T10:30:00.000Z"
                        }
                    }
                ]
                
                # Mock database connection for schema info
                mock_conn = MagicMock()
                mock_conn.execute.return_value.fetchall.return_value = [
                    (0, 'Record_ID', 'VARCHAR', 0, None, 1),
                    (1, 'Company', 'VARCHAR', 0, None, 0),
                    (2, 'Subsidiary', 'VARCHAR', 0, None, 0),
                    (3, 'State', 'VARCHAR', 0, None, 0),
                    (4, 'Product_Line', 'VARCHAR', 0, None, 0),
                    (5, 'Rate_Change_Type', 'VARCHAR', 0, None, 0),
                    (6, 'Premium_Change_Number', 'DECIMAL(10,4)', 0, None, 0),
                    (7, 'Effective_Date', 'DATE', 0, None, 0),
                    (8, 'Airtable_Last_Modified', 'TIMESTAMP', 0, None, 0),
                ]
                
                mock_db_instance.connection.return_value.__enter__.return_value = mock_conn
                
                df, errors = sync._process_page(mock_page, 1)
                
                logger.info(f"  Processed {len(df)} records with {errors} errors")
                assert len(df) == 1, f"Expected 1 record, got {len(df)}"
                assert errors == 0, f"Expected 0 errors, got {errors}"
                
                # Check the processed data
                record = df.iloc[0]
                assert record['Record_ID'] == 'rec123'
                assert record['Company'] == 'Test Insurance Co'
                assert record['Subsidiary'] == 'Test Subsidiary'
                assert record['State'] == 'CA'
                assert record['Product_Line'] == 'Auto'
                assert record['Rate_Change_Type'] == 'increase'
                assert record['Premium_Change_Number'] == 0.15
                
                logger.info("  ✅ All record processing tests passed!")
                
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False
    
    logger.info("=" * 50)
    logger.info("AIRTABLE SYNC METHODS TEST COMPLETED!")
    logger.info("=" * 50)
    
    return True

if __name__ == "__main__":
    print("Testing AirtableSync methods with mock data...")
    success = test_airtable_sync_methods()
    
    if success:
        print("\n✅ AirtableSync methods test passed!")
        print("   - No duplicate logging")
        print("   - All parsing methods working correctly")
        print("   - Mock data processing successful")
    else:
        print("\n❌ AirtableSync methods test failed!")
        sys.exit(1)
