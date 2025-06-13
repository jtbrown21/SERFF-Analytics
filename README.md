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
