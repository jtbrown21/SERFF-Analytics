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

### Creating or Syncing the Database

The project stores data in a DuckDB file at `data/insurance_filings.db`. If the
`data` folder is missing, it will be created automatically when you run the sync
tool.

1. Set your Airtable credentials as environment variables (see
   `src/config.py` for variable names).
2. Run the sync script to create the database and load data:
```bash
python test_sync.py
```
Follow the prompt to run a full sync. This will initialize the database schema
and pull all records from Airtable.

## CSS Development Workflow

A development environment is provided in the `dev/` folder for quickly iterating on the report styling.

1. Open `dev/sample_report.html` in your browser. It links to `dev/style.css` so changes are visible on refresh.
2. Edit `dev/style.css` to adjust styles.
3. Once satisfied, run `./embed_css_in_template.py` to embed the CSS back into `src/agent_report_v2.py`.

The script replaces the contents of the `<style>` block in the template so the production version remains a single file.

## Running Analytics & Health Checks

Once your database is populated you can run various analytics directly from the
command line.

```bash
# Generate the primary agent report
python -m src.agent_report_v2_refined > reports/latest_report.html

# Run basic health checks on the data
python -m src.data_health_checker
```

The health check outputs missing states and duplicate record information so you
can verify the integrity of your filings data.
