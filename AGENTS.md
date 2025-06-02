# AI Agent Instructions - Insurance Analytics Project

## Quick Context

You're helping improve an analytics system for insurance agents. The core value proposition: **Show agents which competitors are raising rates so they can win back customers**. Agents are captive (work for one carrier like State Farm) and use this data to call former customers who switched to competitors that are now raising rates.

## Business Context

- **Users**: Captive insurance agents (State Farm, Allstate, etc.)
- **Pain Point**: Agents don't know when competitors raise rates
- **Solution**: We show them rate increases with timing for outreach
- **Data Source**: Public rate filings from state insurance departments
- **Revenue Model**: Agents pay for competitive intelligence

## Current Architecture
Airtable (5,600+ rate filings)
↓ [Weekly Sync]
DuckDB (Local Analytics DB)
↓ [SQL Analytics]
HTML Reports (Email/Web)

### Key Files

1. **Best Report Version**: `serff_analytics/reports/agent_report_v2_refined.py`
   - Cleanest, most focused version
   - Shows competitor rate increases only
   - Includes action calendar with reminder buttons

2. **Data Pipeline**:
   - `serff_analytics/ingest/airtable_sync.py` - Syncs data from Airtable
   - `serff_analytics/db/utils.py` - DuckDB schema management helpers
   - `serff_analytics/analytics/insights.py` - SQL analytics functions

3. **Other Versions** (for reference):
   - `agent_report.py` - Original version
   - `agent_report_v2.py` - Added revenue focus
   - `agent_report_v3.py` - Tried MJML (too complex)

## Key Design Decisions

1. **No AI/NLP**: Direct SQL is faster and more reliable
2. **Focus on Competitor Rates**: Not showing agent's own rates (too complex)
3. **Simple Tech Stack**: DuckDB + Pandas + Jinja2 templates
4. **State-Level Data**: Rate filings are by state, not zip/county
5. **Email-First Design**: Reports must work in email clients

## Data Schema

```sql
Key fields in 'filings' table:
- Company: Carrier name (has spacing issues)
- State: Full state name (not abbreviation)
- Premium_Change_Number: Decimal (0.05 = 5%)
- Effective_Date: When rate change takes effect
- Policyholders_Affected_Number: Estimated impact
- Product_Line: Type of insurance

## New Reporting Capabilities
- `serff_analytics/reports/state_newsletter.py` generates monthly HTML newsletters for a specific state using `templates/state_newsletter.html`.
- Run with `python -m serff_analytics.reports.state_newsletter <State>` to produce a report file in the `reports/` directory.
- Data is pulled from DuckDB via `DatabaseManager`. Ensure the database is synced before generating reports.
