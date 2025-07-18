# AirtableSync Documentation

## Overview

The `AirtableSync` class provides a robust, production-ready solution for synchronizing data between Airtable and a local DuckDB database. It handles incremental updates, error recovery, data validation, and comprehensive logging.

## Features

- **Incremental Sync**: Only processes records modified since the last successful sync
- **Full Sync**: Complete data refresh when needed
- **Retry Logic**: Automatic retry with exponential backoff for API failures
- **Data Validation**: Comprehensive numeric field validation and parsing
- **Error Handling**: Graceful handling of parsing errors and API failures
- **Sync History**: Complete audit trail of all sync operations
- **Database Optimization**: Automatic index management for performance

## Installation & Configuration

### Prerequisites

```bash
pip install pandas pyairtable tenacity python-dotenv
```

### Environment Variables

Set the following environment variables in your `.env` file:

```env
AIRTABLE_API_KEY=your_api_key_here
AIRTABLE_BASE_ID=your_base_id_here
AIRTABLE_TABLE_NAME=your_table_name_here
DATABASE_PATH=core/data/sources/insurance_filings.db
```

### Database Schema

The sync expects a `filings` table with the following structure:

```sql
CREATE TABLE filings (
    Record_ID VARCHAR PRIMARY KEY,
    Company VARCHAR,
    Subsidiary VARCHAR,
    State VARCHAR,
    Product_Line VARCHAR,
    Rate_Change_Type VARCHAR,
    Premium_Change_Number DOUBLE,
    Premium_Change_Amount_Text VARCHAR,
    Effective_Date DATE,
    Previous_Increase_Date DATE,
    Previous_Increase_Number DOUBLE,
    Policyholders_Affected_Number DOUBLE,
    Policyholders_Affected_Text VARCHAR,
    Total_Written_Premium_Number DOUBLE,
    Total_Written_Premium_Text VARCHAR,
    SERFF_Tracking_Number VARCHAR,
    Specific_Coverages VARCHAR,
    Filing_Method VARCHAR,
    Current_Status VARCHAR,
    Date_Submitted DATE,
    Disposition_Date DATE,
    Stated_Reasons VARCHAR,
    Population VARCHAR,
    Impact_Score DOUBLE,
    Renewals_Date DATE,
    Airtable_Last_Modified TIMESTAMP,
    Created_At TIMESTAMP,
    Updated_At TIMESTAMP
);
```

## Usage

### Basic Usage

```python
from core.data.sync.airtable_sync import AirtableSync

# Create sync instance
sync = AirtableSync()

# Run incremental sync (recommended)
result = sync.sync_data()

# Run full sync (if needed)
result = sync.sync_data(since=datetime(1900, 1, 1))

# Run manual sync from specific date
result = sync.sync_data(since=datetime(2024, 1, 1))
```

### CLI Usage

The module integrates with the CLI interface:

```bash
# Run incremental sync
python -m core.cli.cli sync

# Run full sync
python -m core.cli.cli sync --full

# Check sync status
python -m core.cli.cli status

# View sync history
python -m core.cli.cli history

# Test system health
python -m core.cli.cli test
```

## API Reference

### AirtableSync Class

#### `__init__()`
Initializes the AirtableSync instance with configuration from `core.config.settings`.

#### `sync_data(since: datetime | None = None) -> dict`
Main sync method that synchronizes data from Airtable to DuckDB.

**Parameters:**
- `since` (datetime, optional): Only sync records modified after this datetime. If None, performs incremental sync based on last successful sync.

**Returns:**
- `dict`: Sync result containing:
  - `success`: Boolean indicating sync success
  - `sync_id`: Unique identifier for this sync operation
  - `sync_mode`: 'incremental', 'full', or 'manual'
  - `records_processed`: Number of records fetched from Airtable
  - `records_inserted`: Number of new records inserted
  - `records_updated`: Number of existing records updated
  - `records_skipped`: Number of records skipped (no changes)
  - `total_records`: Total records in database after sync
  - `parsing_errors`: Number of records with parsing errors
  - `failed_inserts`: Number of failed database insertions

**Example:**
```python
sync = AirtableSync()
result = sync.sync_data()
print(f"Processed {result['records_processed']} records")
print(f"Database now has {result['total_records']} total records")
```

#### `diagnose_rate_change_field(sample_size=10) -> tuple`
Diagnoses parsing issues with the "Overall Rate Change Number" field.

**Parameters:**
- `sample_size` (int): Number of records to sample for diagnosis

**Returns:**
- `tuple`: (valid_count, issues_list)

#### `debug_field_values(field_name="Overall Rate Change Number", limit=20) -> None`
Analyzes value types and examples for a specific field.

**Parameters:**
- `field_name` (str): Name of the field to analyze
- `limit` (int): Number of records to analyze

#### `validate_numeric_fields() -> dict`
Validates all numeric fields in the dataset and returns statistics.

**Returns:**
- `dict`: Field validation statistics

## Sync Modes

### Incremental Sync (Default)
- Only processes records modified since the last successful sync
- Automatically determined based on sync history
- Most efficient for regular operations
- Adds 5-minute buffer to handle clock skew

### Full Sync
- Processes all records in the Airtable base
- Used for initial setup or data recovery
- Triggered when no successful sync history exists
- Can be manually triggered with `since=datetime(1900, 1, 1)`

### Manual Sync
- Processes records modified since a specific datetime
- Useful for targeted data recovery or testing
- Triggered by providing a specific `since` parameter

## Error Handling

The module includes comprehensive error handling:

### API Errors
- Automatic retry with exponential backoff (5 attempts)
- Graceful handling of rate limits and network issues
- Detailed error logging with context

### Data Parsing Errors
- Individual record failures don't stop the entire sync
- Parsing errors are logged and counted
- Invalid data is skipped with warnings

### Database Errors
- Transaction rollback on database failures
- Sync history updated with error status
- Database indexes recreated if dropped

## Performance Optimizations

### Database Indexes
- Automatically manages indexes for optimal performance
- Drops indexes during bulk operations
- Recreates indexes after sync completion

### Batch Processing
- Processes records in pages of 100
- Combines all pages before database operations
- Uses DuckDB's UPSERT for efficient updates

### Memory Management
- Streams records from Airtable to minimize memory usage
- Processes data in pandas DataFrames for efficiency
- Automatic cleanup of temporary objects

## Monitoring & Logging

### Sync History
All sync operations are tracked in the `sync_history` table:

```sql
CREATE TABLE sync_history (
    sync_id INTEGER PRIMARY KEY,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR, -- 'running', 'completed', 'failed'
    sync_mode VARCHAR, -- 'incremental', 'full', 'manual'
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_skipped INTEGER,
    parsing_errors INTEGER,
    error_message VARCHAR
);
```

### Logging
The module uses Python's standard logging framework:

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
```

Log levels:
- `INFO`: Sync progress and results
- `WARNING`: Parsing errors and retries
- `ERROR`: Sync failures and exceptions
- `DEBUG`: Detailed diagnostic information

## Field Mapping

### Airtable → Database Field Mapping

| Airtable Field | Database Column | Type | Notes |
|---|---|---|---|
| Record ID | Record_ID | VARCHAR | Primary key |
| Parent Company | Company | VARCHAR | |
| Filing Company | Subsidiary | VARCHAR | |
| Impacted State | State | VARCHAR | |
| Product Line | Product_Line | VARCHAR | |
| Rate Change Type | Rate_Change_Type | VARCHAR | |
| Overall Rate Change Number | Premium_Change_Number | DOUBLE | Parsed from percentage |
| Overall Rate Change | Premium_Change_Amount_Text | VARCHAR | |
| Effective Date | Effective_Date | DATE | Multiple date fields tried |
| Previous Increase Date | Previous_Increase_Date | DATE | |
| Previous Increase Number | Previous_Increase_Number | DOUBLE | |
| Policyholders Affected Number | Policyholders_Affected_Number | DOUBLE | |
| Policyholders Affected Text | Policyholders_Affected_Text | VARCHAR | |
| Total Written Premium Number | Total_Written_Premium_Number | DOUBLE | |
| Total Written Premium Text | Total_Written_Premium_Text | VARCHAR | |
| SERFF Tracking Number | SERFF_Tracking_Number | VARCHAR | |
| Specific Coverages | Specific_Coverages | VARCHAR | |
| Filing Method | Filing_Method | VARCHAR | |
| Current Status | Current_Status | VARCHAR | |
| Date Submitted | Date_Submitted | DATE | |
| Disposition Date | Disposition_Date | DATE | |
| Name | Stated_Reasons | VARCHAR | |
| Population | Population | VARCHAR | |
| Impact Score | Impact_Score | DOUBLE | |
| Renewals Date | Renewals_Date | DATE | |
| Last Modified | Airtable_Last_Modified | TIMESTAMP | Used for incremental sync |

### Data Type Handling

#### Numeric Fields
- Percentages are handled as decimals (0.179 = 17.9%)
- Empty strings and null values become NULL
- Invalid numeric values are logged and skipped

#### Date Fields
- Multiple date formats supported
- Invalid dates become NULL
- ISO format preferred

#### Text Fields
- Empty strings preserved
- Null values become empty strings
- Text length not limited

## Troubleshooting

### Common Issues

#### "No records to process"
- Normal for incremental sync when no changes exist
- Check Airtable for recent modifications
- Verify Last Modified field is populated

#### "Failed to parse number"
- Check numeric field formats in Airtable
- Use `diagnose_rate_change_field()` for analysis
- Verify percentage fields are properly formatted

#### "Database does not exist"
- Ensure database path is correct in configuration
- Check file permissions
- Verify database schema is initialized

#### "Airtable API authentication failed"
- Verify AIRTABLE_API_KEY is correct
- Check API key permissions
- Ensure base ID and table name are correct

### Diagnostic Commands

```python
# Check field value formats
sync.debug_field_values("Overall Rate Change Number")

# Validate all numeric fields
stats = sync.validate_numeric_fields()

# Diagnose specific field issues
valid_count, issues = sync.diagnose_rate_change_field(sample_size=50)
```

### Database Queries

```sql
-- Check sync history
SELECT * FROM sync_history ORDER BY started_at DESC LIMIT 10;

-- Count records by status
SELECT status, COUNT(*) FROM sync_history GROUP BY status;

-- Find parsing errors
SELECT * FROM sync_history WHERE parsing_errors > 0;

-- Check data quality
SELECT COUNT(*) as total_records,
       COUNT(Premium_Change_Number) as with_rate_change,
       COUNT(Effective_Date) as with_effective_date
FROM filings;
```

## Best Practices

### Regular Operations
1. Run incremental sync daily or hourly
2. Monitor sync history for failures
3. Set up alerts for consecutive failures
4. Review parsing errors periodically

### Data Quality
1. Use validation methods to check data integrity
2. Monitor parsing error rates
3. Verify numeric field formats in Airtable
4. Ensure required fields are populated

### Performance
1. Use incremental sync for regular operations
2. Schedule full sync during off-peak hours
3. Monitor database size and performance
4. Consider archiving old sync history

### Error Recovery
1. Check sync history for failure patterns
2. Use manual sync for targeted recovery
3. Verify Airtable data before re-syncing
4. Test with small date ranges first

## Integration Examples

### Workflow Integration
```python
from core.data.sync.airtable_sync import AirtableSync
from core.workflows.monthly_workflow import MonthlyWorkflow

# Sync data first
sync = AirtableSync()
result = sync.sync_data()

if result['success']:
    # Run monthly workflow with fresh data
    workflow = MonthlyWorkflow()
    workflow.run()
```

### Scheduled Sync
```python
import schedule
import time

def daily_sync():
    sync = AirtableSync()
    result = sync.sync_data()
    if not result['success']:
        # Send alert or log error
        logger.error(f"Daily sync failed: {result}")

schedule.every().day.at("02:00").do(daily_sync)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Data Validation Pipeline
```python
def validate_and_sync():
    sync = AirtableSync()
    
    # Run validation first
    stats = sync.validate_numeric_fields()
    
    # Check for high error rates
    total_errors = sum(field['invalid'] for field in stats.values())
    if total_errors > 100:
        logger.warning(f"High error rate detected: {total_errors} invalid records")
        return False
    
    # Run sync if validation passes
    result = sync.sync_data()
    return result['success']
```

## Version History

- **v1.0**: Initial implementation with basic sync functionality
- **v1.1**: Added retry logic and error handling
- **v1.2**: Implemented incremental sync and sync history
- **v1.3**: Added comprehensive data validation
- **v1.4**: Performance optimizations and index management
- **v1.5**: Integration with new core architecture
