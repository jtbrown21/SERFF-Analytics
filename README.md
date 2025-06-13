# SERFF Analytics

See [docs/README.md](docs/README.md) for project documentation.

## CLI Commands

The project includes a CLI for managing Airtable syncs:

### Installation
```bash
pip install -r requirements.txt
```

### Usage

```bash
# Run incremental sync (only updates changed records)
python -m serff_analytics.cli sync

# Force full sync (updates all records)
python -m serff_analytics.cli sync --full

# Check sync status
python -m serff_analytics.cli status

# View sync history
python -m serff_analytics.cli history

# Test system health
python -m serff_analytics.cli test
```

### Quick Start

1. First test the system:
   ```bash
   python -m serff_analytics.cli test
   ```

2. Check current status:
   ```bash
   python -m serff_analytics.cli status
   ```

3. Run a sync:
   ```bash
   python -m serff_analytics.cli sync
   ```

### Automation

Add to crontab for hourly syncs:
```bash
0 * * * * cd /path/to/project && python -m serff_analytics.cli sync
```

## Title: Implement Incremental Sync with Modification Tracking for Airtable Integration

### Summary
This PR implements a robust incremental sync system that dramatically improves sync performance by only updating records that have been modified in Airtable since the last sync. Previously, every sync would update all 36k+ records. Now, incremental syncs complete in under 1 second when no changes are detected.

### Problem Solved
- **Duplicate key constraint errors** when syncing Airtable data to DuckDB
- **Performance issues** - every sync updated all records (90+ seconds)
- **No sync tracking** - couldn't tell when data was last updated
- **Poor observability** - no way to monitor sync health or history

### Solution Implemented

#### 1. Database Schema Changes
- Added `Airtable_Last_Modified` column to track record modification times
- Created `sync_history` table for comprehensive sync tracking
- Worked around DuckDB PRIMARY KEY bug by using UNIQUE constraint

#### 2. Incremental Sync Logic
- Automatic sync mode detection (full vs incremental)
- Only fetches records modified since last successful sync (with 5-minute buffer)
- UPSERT with conditional WHERE clause - only updates if Airtable timestamp is newer
- Proper handling of empty sync results

#### 3. CLI Interface
```bash
python -m serff_analytics.cli sync          # Run sync (auto-detects mode)
python -m serff_analytics.cli sync --full   # Force full sync
python -m serff_analytics.cli status        # Check sync status
python -m serff_analytics.cli history       # View sync history
python -m serff_analytics.cli test          # Run system health checks
```

#### 4. Performance Improvements
- **Before**: Every sync processed 36k records (~90 seconds)
- **After**: 
  - Initial full sync: ~90 seconds
  - Incremental sync (no changes): <1 second
  - Incremental sync (few changes): 2-5 seconds

### Technical Details

#### Key Code Changes
- `airtable_sync.py`: 
  - Fixed pagination bug (was fetching records twice)
  - Added date parsing for ISO format timestamps
  - Implemented UPSERT with modification checking
  - Added comprehensive error handling and logging
- `cli.py`: New CLI interface with status, history, and test commands
- Database fixes for DuckDB-specific issues

#### Breaking Changes
None - existing sync functionality preserved with `--full` flag

### Testing
```bash
# 1. Set up fresh database
python setup_db_fixed.py

# 2. Run initial full sync
python -m serff_analytics.cli sync --full

# 3. Test incremental sync
python -m serff_analytics.cli sync

# 4. Verify system health
python -m serff_analytics.cli test
```

### Migration Notes
For existing installations:
1. Run `ALTER TABLE filings ADD COLUMN Airtable_Last_Modified TIMESTAMP`
2. Create sync_history table (see setup_db_fixed.py)
3. First sync will be full, subsequent syncs will be incremental

---

# Updated README.md Section

Add this section to your README.md:

```markdown
## Airtable Sync System

This project includes a robust incremental sync system for keeping DuckDB in sync with Airtable data.

### Features

- **Incremental Updates**: Only syncs records modified since the last sync
- **Performance Optimized**: Sub-second syncs when no changes detected
- **Full Audit Trail**: Complete history of all sync operations
- **CLI Interface**: Simple commands for all sync operations
- **Error Recovery**: Gracefully handles failures with detailed logging

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set up the database (first time only)
python setup_db_fixed.py
```

### Configuration

Set the following environment variables:
```bash
export AIRTABLE_API_KEY="your_api_key"
export AIRTABLE_BASE_ID="your_base_id"
export AIRTABLE_TABLE_NAME="your_table_name"
```

### Usage

#### Basic Commands

```bash
# Run sync (automatically detects if full or incremental is needed)
python -m serff_analytics.cli sync

# Force a full sync
python -m serff_analytics.cli sync --full

# Check sync status
python -m serff_analytics.cli status

# View sync history
python -m serff_analytics.cli history

# Run system health tests
python -m serff_analytics.cli test
```

#### Example Output

```
$ python -m serff_analytics.cli sync
🔄 Starting sync...
   Mode: INCREMENTAL
✅ Sync completed in 0.8 seconds!
   Processed: 5 records
   Total in DB: 36,331

$ python -m serff_analytics.cli status
📊 SYNC STATUS
==================================================
Last sync: ✅ COMPLETED (incremental)
Started: 2025-06-13 14:23:45
Records in DB: 36,331
With timestamps: 36,331 (100.0%)
✅ Last sync was 0.5 hours ago
```

### Sync Logic

1. **First Sync**: Performs a full sync of all records
2. **Subsequent Syncs**: 
   - Checks the last successful sync time
   - Only fetches records where `Last Modified` > (last_sync_time - 5 minutes)
   - Updates only if Airtable's timestamp is newer than the database timestamp
3. **Forced Full Sync**: Use `--full` flag to sync all records regardless of modification time

### Database Schema

The sync system adds these elements to your database:

```sql
-- Added to filings table
Airtable_Last_Modified TIMESTAMP  -- Tracks when record was last modified in Airtable

-- Sync history tracking
CREATE TABLE sync_history (
    sync_id INTEGER,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    sync_mode VARCHAR(50),      -- 'full' or 'incremental'
    status VARCHAR(50),         -- 'running', 'completed', 'failed'
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_skipped INTEGER,
    parsing_errors INTEGER
);
```

### Automation

To run syncs automatically, add to your crontab:

```bash
# Run sync every hour
0 * * * * cd /path/to/project && python -m serff_analytics.cli sync >> sync.log 2>&1

# Run full sync weekly (Sunday at 2 AM)
0 2 * * 0 cd /path/to/project && python -m serff_analytics.cli sync --full >> sync.log 2>&1
```

### Troubleshooting

#### Database Lock Error
If you see "Conflicting lock is held", close any open DuckDB CLI sessions:
```bash
ps aux | grep duckdb
kill <PID>
```

#### Sync Seems Stuck
The sync includes a safety limit. If it's processing too many pages, check:
1. Your Airtable base/table configuration
2. Whether you're connected to the correct table
3. Run `python -m serff_analytics.cli test` to verify system health

#### All Records Updating Every Time
This usually means the `Airtable_Last_Modified` column isn't being populated:
1. Verify your Airtable has a "Last Modified" field
2. Check date parsing in logs for any warnings
3. Run a full sync to repopulate timestamps

### Performance Metrics

Typical sync performance (36k records):
- **Full sync**: ~90 seconds
- **Incremental (no changes)**: <1 second  
- **Incremental (100 changes)**: 2-5 seconds
- **Incremental (1000 changes)**: 10-15 seconds

### Contributing

When modifying the sync system:
1. Test with both full and incremental syncs
2. Verify sync_history is updated correctly
3. Run `python -m serff_analytics.cli test` to ensure system health
4. Check that indexes are properly managed during sync
```
