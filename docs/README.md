# Insurance Rate Filing Analytics

An intelligent analytics system for insurance agents to track competitor rate changes and identify win-back opportunities.

## Overview

This system syncs insurance rate filing data from Airtable and generates actionable intelligence reports for insurance agents, showing:
- Which competitors are raising rates
- When rate changes take effect  
- Estimated affected policies
- Optimal outreach timing

## Features

- **Automated Data Sync**: Weekly sync from Airtable to local DuckDB
- **Intelligent Reports**: HTML reports focused on competitive intelligence
- **Multiple Report Versions**: From detailed analytics to simplified actionable insights
- **Direct SQL Analytics**: No AI/NLP needed - pure data analysis

## Setup

1. Clone the repository
```bash
git clone https://github.com/USERNAME/insurance-analytics.git
cd insurance-analytics
```

2. Activate the provided virtual environment (all dependencies are pre-installed)
```bash
source venv/bin/activate
```
If `venv/` is missing or you prefer your own environment, run the setup script:
```bash
./scripts/setup_dev_env.sh
```
This creates a virtual environment and installs packages from `requirements.txt`.

3. Verify dependencies can be imported:
```bash
python scripts/verify_deps.py
```

### Creating or Syncing the Database

The project stores data in a DuckDB file at `data/insurance_filings.db`. If the
`data` folder is missing, it will be created automatically when you run the sync
tool.

1. Set your Airtable credentials as environment variables (see
   `serff_analytics/config.py` for variable names).
2. Run the sync script to create the database and load data:
```bash
python scripts/sync_demo.py
```
Follow the prompt to run a full sync. This will initialize the database schema
and pull all records from Airtable.

## CSS Development Workflow

A development environment is provided in the `dev/` folder for quickly iterating on the report styling.

1. Open `dev/sample_report.html` in your browser. It links to `dev/style.css` so changes are visible on refresh.
2. Edit `dev/style.css` to adjust styles.
3. Once satisfied, run `python scripts/embed_css_in_template.py` to embed the CSS back into `serff_analytics/reports/agent_report_v2.py`.

The script replaces the contents of the `<style>` block in the template so the production version remains a single file.

## Running Analytics & Health Checks

Once your database is populated you can run various analytics directly from the
command line.

```bash
# Generate the primary agent report
python -m serff_analytics.reports.agent_report_v2_refined > reports/latest_report.html

# Run basic health checks on the data
python -m serff_analytics.health.data_health_check --overview --year 2024
```

The health check outputs missing states and duplicate record information so you
can verify the integrity of your filings data.

## State-Specific Newsletter Reports

This project now includes a monthly HTML newsletter template for any U.S. state. Reports pull data directly from the DuckDB database and render using Jinja2.

### Generating a Report

```bash
python -m serff_analytics.reports.state_newsletter Illinois --month 2024-03 > reports/illinois_march_2024.html
```

To see all database queries executed, add the `--test` flag:

```bash
python -m serff_analytics.reports.state_newsletter Illinois --month 2024-03 --test > reports/illinois_march_2024.html
```

### Data Flow

```
Database (DuckDB) --> state_newsletter.py --> templates/state_newsletter.html --> HTML output
```

Reports are generated for a single state at a time. Use the optional `--month` flag to target a specific month (`YYYY-MM`). The `--test` flag enables verbose logging of queries. The output file name now includes the state and month for clarity.

## Troubleshooting

If you encounter an error like `duckdb.duckdb.IOException: IO Error: Cannot open file` when generating reports, the database file is likely missing. Run `python scripts/sync_demo.py` to create the database and pull data from Airtable.
