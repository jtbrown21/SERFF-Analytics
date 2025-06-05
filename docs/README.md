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
- **Automated Newsletter System**: Monthly email delivery with full tracking
- **Subscriber Management**: Airtable-based CRM with unsubscribe handling
- **Email Analytics**: Automatic tracking of delivery, opens, and engagement

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

### Environment Variables

Create a `.env` file in the project root with:
```bash
# Airtable Configuration
AIRTABLE_API_KEY=your-airtable-api-key
AIRTABLE_BASE_ID=your-base-id

# Email Configuration
POSTMARK_SERVER_TOKEN=your-postmark-token

# GitHub Configuration (for report URLs)
GITHUB_USERNAME=your-github-username
GITHUB_REPO_NAME=insurance-analytics

# Security
UNSUBSCRIBE_SECRET=your-secret-key
```

### Creating or Syncing the Database

The project stores data in a DuckDB file at `serff_analytics/data/insurance_filings.db`. If the
`serff_analytics/data` folder is missing, it will be created automatically when you run the sync
tool.

1. Set your Airtable credentials as environment variables (see
   `serff_analytics/config.py` for variable names).
2. Run the sync script to create the database and load data:
```bash
python -m scripts/sync_demo
```
Follow the prompt to run a full sync. This will initialize the database schema
and pull all records from Airtable.

## Newsletter Workflow

### Monthly Newsletter System

The system includes a complete newsletter workflow that:
1. Generates state-specific reports on the 29th of each month
2. Tracks reports in Airtable for manual review
3. Sends approved reports via email on the 1st of the month
4. Tracks all email engagement automatically

### Airtable Structure

Create these tables in your Airtable base:

#### Reports Table
- Name (Text)
- State (Single Select)
- Month (Single Select)
- Year (Text)
- Report URL (URL)
- Status (Single Select: Generated, Approved, Sent)

#### Subscribers Table
- Name (Text)
- Email (Email)
- State (Single/Multiple Select)
- Active (Checkbox)
- Test (Checkbox)
- Unsubscribe Date (Date & Time)
- Unsubscribe Method (Single Select)

#### Emails Table
- Subscribers (Link to Subscribers)
- Report (Link to Reports)
- Subject Line (Text)
- Sent Date (Date & Time)
- Postmark Message ID (Text)
- Status (Single Select: Sent, Delivered, Opened, Bounced, Failed)
- Email Type (Single Select: Test, Monthly Newsletter)
- Opened Date (Date & Time)
- Notes (Long Text)

### GitHub Pages Setup

Reports are hosted on GitHub Pages for easy access:

1. Enable GitHub Pages in your repository settings
2. Set source to deploy from main branch, `/docs` folder
3. Reports will be accessible at: `https://[username].github.io/[repo-name]/newsletters/monthly/19.0/`

### Email Delivery Setup

1. Sign up for [Postmark](https://postmarkapp.com)
2. Create a server and get your API token
3. Verify your sending domain
4. Add token to `.env` file

### Webhook Setup for Email Tracking

The system includes automatic email tracking via webhooks:

1. Deploy webhook handler to Render:
```bash
# Webhook handler is in src/webhook_handler.py
# Deploy to Render.com for free hosting
```

2. Configure Postmark webhooks:
   - URL: `https://your-app.onrender.com/webhook/postmark`
   - Select events: Delivery, Open, Bounce, Spam Complaint

3. Unsubscribe handling:
   - Automatic unsubscribe links in every email
   - Hosted unsubscribe page
   - Updates subscriber status in Airtable

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
python -m serff_analytics.reports.state_newsletter Illinois --month 2024-03
```

The report is saved to `docs/newsletters/monthly/19.0/IL/2024/March/IL_03_2024.html`, committed to GitHub, and logged in Airtable.

To generate without pushing or logging, add the `--test` flag:

```bash
python -m serff_analytics.reports.state_newsletter Illinois --month 2024-03 --test
```

### Newsletter Workflow Commands

```bash
# Test email sending with embedded report
python -m scripts.test_subscriber_tracking

# Generate reports for all states (run on 29th)
python -m src.monthly_workflow generate

# Send approved reports (run on 1st)
python -m src.monthly_workflow send

# Test webhook locally
python src/webhook_handler.py
```

### Data Flow

```
Database (DuckDB) --> state_newsletter.py --> HTML Report --> GitHub Pages
                                                    |
                                                    v
                                              Airtable (tracking)
                                                    |
                                                    v
                                              Email (Postmark)
                                                    |
                                                    v
                                            Webhook (tracking updates)
```

## Key Python Modules

### src/report_manager.py
Manages Airtable operations for the Reports table:
- `log_report()` - Creates report records
- `get_approved_reports()` - Finds approved reports for sending
- `mark_as_sent()` - Updates status after email delivery

### src/email_service.py
Handles all email operations via Postmark:
- `send_newsletter_embedded_with_subscriber_tracking()` - Sends emails with full HTML embedded
- `get_test_subscribers()` - Retrieves test subscribers for safe testing
- Includes unsubscribe link generation

### src/generate_reports.py
Generates monthly state reports and logs them to Airtable.

### src/send_reports.py
Sends approved reports to subscribers with tracking.

### src/webhook_handler.py
Flask application for receiving Postmark webhooks:
- Tracks email delivery status
- Records when emails are opened
- Handles unsubscribe requests
- Updates Airtable automatically

## Testing

### Test Mode
Set subscribers with Test = TRUE in Airtable to safely test the workflow without sending to all subscribers.

### Local Testing
```bash
# Test email sending
python -m scripts.test_subscriber_tracking

# Test webhook handling
python src/webhook_handler.py  # Terminal 1
python test_manual_webhook.py   # Terminal 2
```

### Production Testing
1. Generate a test report for one state
2. Mark as Approved in Airtable
3. Send to test subscribers only
4. Verify tracking updates work

## Deployment

### GitHub Pages (Reports)
Reports are automatically available when pushed to the `/docs` folder.

### Render (Webhook Handler)
1. Connect GitHub repo to Render
2. Deploy as a Web Service
3. Add environment variables
4. Use the public URL for Postmark webhooks

## Troubleshooting

### Database Issues
If you encounter an error like `duckdb.duckdb.IOException: IO Error: Cannot open file` when generating reports, the database file is likely missing. Run `python scripts/sync_demo.py` to create the database and pull data from Airtable.

Older installations may store `Premium_Change_Number` with only two decimal places. The current schema uses four decimal places. Any script that initializes `DatabaseManager` will automatically apply the following migration:

```sql
ALTER TABLE filings ALTER COLUMN Premium_Change_Number SET DATA TYPE DECIMAL(10,4);
```

### Email Issues
- Check Postmark dashboard for delivery status
- Verify sender domain is authenticated
- Check webhook handler logs on Render

### Airtable Issues
- Ensure field names match exactly (spaces matter!)
- Check API key and base ID are correct
- Verify linked records are properly set up

## CSS Development Workflow

A development environment is provided in the `dev/` folder for quickly iterating on the report styling.

1. Open `dev/sample_report.html` in your browser. It links to `dev/style.css` so changes are visible on refresh.
2. Edit `dev/style.css` to adjust styles.
3. Once satisfied, run `python scripts/embed_css_in_template.py` to embed the CSS back into `serff_analytics/reports/agent_report_v2.py`.

The script replaces the contents of the `<style>` block in the template so the production version remains a single file.

## Monthly Automation (Coming Soon)

Set up automated monthly runs using GitHub Actions or cron:
- 29th: Generate all state reports
- 30th: Manual review period
- 1st: Send approved reports

## Contributing

When adding new features:
1. Test with Test = TRUE subscribers first
2. Verify Airtable tracking works correctly
3. Check webhook integration
4. Update documentation