# AI Agent Quick Reference

## Environment Setup
- Python 3.x with virtual environment at `./venv`
- All dependencies pre-installed (no internet access)
- DuckDB database initialized at `data/insurance_filings.db`

## Key Commands
```bash
# Activate virtual environment
source venv/bin/activate

# Run tests
python scripts/run_tests.py

# Format code
python format_code.py

# Generate report
python -m serff_analytics.reports.agent_report_v2_refined

# Check data
python scripts/check_data.py
