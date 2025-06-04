# Code X Agent Instructions - SERFF Analytics

## Quick Context
You're improving a competitive intelligence system for captive insurance agents. The goal is to **show agents which competitors are raising rates so they can win back customers**.

## Business Context
- **Users**: Captive insurance agents (State Farm, Allstate, etc.)
- **Pain Point**: Agents don't know when competitors raise rates
- **Solution**: Provide timely insights about competitor rate increases
- **Data Source**: Public rate filings from state insurance departments
- **Revenue Model**: Agents pay for access to these insights

## Current Architecture
Airtable (5,600+ rate filings)
↓ [Weekly Sync]
DuckDB (Local Analytics DB)
↓ [SQL Analytics]
HTML Reports (Email/Web)

### Key Files
1. **Best Report Version**: `serff_analytics/reports/agent_report_v2_refined.py`
   - Cleanest version focused on competitor rate increases
   - Includes action calendar with reminder buttons
2. **Data Pipeline**:
   - `serff_analytics/ingest/airtable_sync.py` – syncs data from Airtable
   - `serff_analytics/db/utils.py` – DuckDB schema helpers
   - `serff_analytics/analytics/insights.py` – SQL analytics functions
3. **Other Versions** (for reference):
   - `agent_report.py` – original report
   - `agent_report_v2.py` – added revenue focus
   - `agent_report_v3.py` – tried MJML (too complex)

## Key Design Decisions
1. **No AI/NLP** – direct SQL analytics is faster and more reliable
2. **Focus on Competitor Rates** – we don't show the agent's own rate changes
3. **Simple Tech Stack** – DuckDB + Pandas + Jinja2 templates
4. **State-Level Data** – filings are by state, not zip/county
5. **Email-First Design** – reports must render in email clients

## Data Schema
```sql
Key fields in 'filings' table:
- Company: Carrier name (may have spacing issues)
- State: Full state name (not abbreviation)
- Premium_Change_Number: Decimal (0.05 = 5%)
- Effective_Date: When the rate change takes effect
- Policyholders_Affected_Number: Estimated impact
- Product_Line: Type of insurance
```

## New Reporting Capabilities
- `serff_analytics/reports/state_newsletter.py` generates a monthly HTML newsletter for a specific state using `templates/state_newsletter.html`.
- Run with `python -m serff_analytics.reports.state_newsletter <State>` to produce a report file under `docs/newsletters/monthly/19.0/<STATE>/<YEAR>/<MONTH>/`.
- Data is pulled from DuckDB via `DatabaseManager`. Ensure the database is synced before generating reports.
- `src/monthly_workflow.py` orchestrates the entire newsletter workflow.
- `src/generate_reports.py` builds monthly HTML reports for all active states.
- `src/send_reports.py` delivers approved reports via email.
- Use `python -m src.monthly_workflow generate` to create state reports.
- Use `python -m src.monthly_workflow send` to email the approved reports.
- These commands automate the generation and delivery of state reports.

## Environment Setup
- Python 3.x with a virtual environment located at `./venv`
- All dependencies are pre‑installed (no internet access)
- DuckDB database at `data/insurance_filings.db`

## Key Commands
```bash
# Activate the virtual environment
source venv/bin/activate

# Run tests
python scripts/run_tests.py

# Format code
python format_code.py

# Generate the main agent report
python -m serff_analytics.reports.agent_report_v2_refined > reports/latest_report.html

# Check data health
python scripts/check_data.py
```
