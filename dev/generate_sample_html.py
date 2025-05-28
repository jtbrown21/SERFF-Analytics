from jinja2 import Template
import json

# Load template with Jinja syntax (with external CSS link)
with open('dev/agent_report_dev.html') as f:
    tmpl = Template(f.read())

# Sample data for development
sample_data = {
    'agent_carrier': 'State Farm',
    'state': 'Arizona',
    'report_date': 'April 10, 2024',
    'total_opportunities': 3,
    'urgent_opportunities': 1,
    'avg_competitor_increase': 4.5,
    'max_increase': 8.2,
    'estimated_policies': '12,000',
    'timestamp': '12:00 PM',
    'data_freshness': 'April 9, 2024',
    'product_options': ['Auto', 'Home', 'Life'],
    'opportunities': [
        {
            'competitor': 'Acme Insurance',
            'rate_increase': 5.4,
            'effective_date': 'May 15, 2024',
            'days_until': 34,
            'policies_affected': '8,000',
            'product_lines': 'Auto',
            'filing_count': 3,
            'recent_changes': [2.1, 3.2, 4.5],
            'urgency_percent': 70,
            'urgency_color': '#10b981',
            'call_script': 'Call former customers when Acme Insurance raises rates 5.4%.'
        },
        {
            'competitor': 'Beta Mutual',
            'rate_increase': 8.2,
            'effective_date': 'April 20, 2024',
            'days_until': 9,
            'policies_affected': '5,500',
            'product_lines': 'Home, Auto',
            'filing_count': 2,
            'recent_changes': [5.0, 3.5],
            'urgency_percent': 90,
            'urgency_color': '#dc2626',
            'call_script': 'Call former customers when Beta Mutual raises rates 8.2%.'
        },
        {
            'competitor': 'Capital Casualty',
            'rate_increase': 4.0,
            'effective_date': 'June 1, 2024',
            'days_until': 51,
            'policies_affected': '10,000',
            'product_lines': 'Auto',
            'filing_count': 4,
            'recent_changes': [2.5, 3.0, 3.8],
            'urgency_percent': 60,
            'urgency_color': '#10b981',
            'call_script': 'Call former customers when Capital Casualty raises rates 4.0%.'
        }
    ],
    'timeline': [
        {
            'carrier': 'Beta Mutual',
            'effective_date': 'Apr 20',
            'outreach_date': 'Mar 21',
            'days_until': 9
        },
        {
            'carrier': 'Acme Insurance',
            'effective_date': 'May 15',
            'outreach_date': 'Apr 15',
            'days_until': 34
        },
        {
            'carrier': 'Capital Casualty',
            'effective_date': 'Jun 1',
            'outreach_date': 'May 2',
            'days_until': 51
        }
    ]
}

rendered = tmpl.render(**sample_data)

with open('dev/sample_report.html', 'w') as f:
    f.write(rendered)
