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
python run_tests.py

# Format code
python format_code.py

# Generate report
python -m src.agent_report_v2_refined

# Check data
python check_data.py
