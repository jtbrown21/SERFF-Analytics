# src/agent_report_v2_refined.py
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from jinja2 import Template
import os


class AgentIntelligenceReportV2Refined:
    def __init__(self, db_path="serff_analytics/data/insurance_filings.db"):
        self.db_path = db_path
        self.report_template = Template(
            """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Rate Increase Intelligence - {{ state }}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f0f2f5;
            color: #1a1a1a;
            line-height: 1.6;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        /* Header */
        .header {
            background: linear-gradient(135deg, #1e3a8a 0%, #3730a3 100%);
            color: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .header h1 {
            font-size: 28px;
            margin-bottom: 10px;
        }
        .header-meta {
            display: flex;
            gap: 30px;
            font-size: 14px;
            opacity: 0.9;
        }
        
        /* Alert Box */
        .alert-box {
            background: #fef3c7;
            border: 2px solid #f59e0b;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 30px;
            display: flex;
            align-items: center;
            gap: 15px;
        }
        .alert-icon {
            font-size: 24px;
        }
        
        /* Summary Section */
        .summary-section {
            background: white;
            border-radius: 12px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 30px;
            margin-top: 20px;
        }
        .summary-stat {
            text-align: center;
            padding: 20px;
            background: #f9fafb;
            border-radius: 8px;
        }
        .summary-number {
            font-size: 36px;
            font-weight: 700;
            color: #1e3a8a;
        }
        .summary-label {
            font-size: 14px;
            color: #6b7280;
            margin-top: 5px;
        }

        /* Filter Bar */
        .filter-bar {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 20px;
        }
        .filter-bar select,
        .filter-bar input {
            padding: 6px 10px;
            border: 1px solid #d1d5db;
            border-radius: 6px;
            font-size: 14px;
        }
        .filter-bar button {
            background: #4f46e5;
            color: #fff;
            border: none;
            padding: 6px 12px;
            border-radius: 6px;
            cursor: pointer;
        }
        
        /* Opportunity Cards */
        .opportunities-grid {
            display: grid;
            gap: 20px;
            margin-bottom: 30px;
        }
        .opportunity-card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            border-left: 4px solid #10b981;
            transition: transform 0.2s;
            position: relative;
        }
        .opportunity-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        .opportunity-header {
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 15px;
        }
        .competitor-name {
            font-size: 20px;
            font-weight: 600;
            color: #1e3a8a;
        }
        .rate-badge {
            background: #dc2626;
            color: white;
            padding: 6px 16px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 18px;
        }
        
        .opportunity-details {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid #e5e7eb;
        }
        .detail-item {
            text-align: center;
        }
        .detail-label {
            font-size: 12px;
            color: #6b7280;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .detail-value {
            font-size: 18px;
            font-weight: 600;
            margin-top: 5px;
            color: #111827;
        }

        .progress-bar {
            height: 6px;
            background: #e5e7eb;
            border-radius: 3px;
            overflow: hidden;
            margin-top: 15px;
        }
        .progress-fill {
            height: 100%;
            background: #10b981;
        }

        .toggle-btn {
            background: transparent;
            border: none;
            color: #3b82f6;
            cursor: pointer;
            margin-top: 10px;
            font-size: 14px;
        }
        .extra-details {
            display: none;
            margin-top: 15px;
            border-top: 1px dashed #d1d5db;
            padding-top: 15px;
        }
        .sparkline {
            margin-top: 5px;
        }
        
        /* Action Timeline */
        .timeline {
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .timeline-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px;
            margin-bottom: 15px;
            background: #f9fafb;
            border-radius: 8px;
            border: 1px solid #e5e7eb;
        }
        .timeline-item:hover {
            background: #f3f4f6;
            border-color: #d1d5db;
        }
        .timeline-content {
            flex: 1;
        }
        .timeline-carrier {
            font-size: 18px;
            font-weight: 600;
            color: #111827;
            margin-bottom: 5px;
        }
        .timeline-meta {
            font-size: 14px;
            color: #6b7280;
        }
        .timeline-date {
            font-size: 16px;
            font-weight: 600;
            color: #1e3a8a;
            margin-right: 20px;
        }
        .timeline-badge {
            display: inline-block;
            padding: 2px 8px;
            background: #dbeafe;
            color: #1e40af;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            margin-left: 10px;
        }
        .timeline-badge.urgent {
            background: #fee2e2;
            color: #dc2626;
        }
        
        .reminder-btn {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            transition: background 0.2s;
            white-space: nowrap;
        }
        .reminder-btn:hover {
            background: #2563eb;
        }
        .reminder-btn.saved {
            background: #10b981;
        }
        .reminder-btn.saved:hover {
            background: #059669;
        }
        
        /* Footer */
        .footer {
            text-align: center;
            padding: 20px;
            color: #6b7280;
            font-size: 12px;
            margin-top: 40px;
        }
        
        @media (max-width: 768px) {
            .opportunity-details {
                grid-template-columns: 1fr;
            }
            .summary-grid {
                grid-template-columns: 1fr;
            }
            .timeline-item {
                flex-direction: column;
                align-items: start;
                gap: 15px;
            }
            .reminder-btn {
                width: 100%;
            }
        }
        
        @media print {
            body { background: white; font-size: 12px; line-height: 1.3; }
            .container { max-width: 100%; }
            .opportunity-card { break-inside: avoid; margin-bottom: 20px; }
            .reminder-btn { display: none; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{{ total_opportunities }} Competitor Rate Increases</h1>
            <div class="header-meta">
                <span>📍 {{ state }}</span>
                <span>📅 {{ report_date }}</span>
                <span>🏢 {{ agent_carrier }} Agent</span>
            </div>
        </div>
        
        {% if urgent_opportunities %}
        <div class="alert-box">
            <span class="alert-icon">⚡</span>
            <div>
                <strong>{{ urgent_opportunities }} rate increases taking effect within 30 days.</strong>
                These represent immediate opportunities to contact affected customers.
            </div>
        </div>
        {% endif %}

        <div class="filter-bar">
            <input type="text" id="carrierFilter" placeholder="Filter carrier">
            <select id="productFilter">
                <option value="">All Products</option>
                {% for prod in product_options %}
                <option value="{{ prod }}">{{ prod }}</option>
                {% endfor %}
            </select>
            <select id="sortFilter">
                <option value="days">Sort by Days Until</option>
                <option value="increase">Sort by Increase</option>
            </select>
            <button onclick="applyFilter()">Apply</button>
        </div>

        <div class="summary-section">
            <h2>Market Summary</h2>
            <div class="summary-grid">
                <div class="summary-stat">
                    <div class="summary-number">{{ total_opportunities }}</div>
                    <div class="summary-label">Competitors<br>Raising Rates</div>
                </div>
                <div class="summary-stat">
                    <div class="summary-number">{{ avg_competitor_increase }}%</div>
                    <div class="summary-label">Average<br>Rate Increase</div>
                </div>
                <div class="summary-stat">
                    <div class="summary-number">{{ max_increase }}%</div>
                    <div class="summary-label">Highest<br>Rate Increase</div>
                </div>
                <div class="summary-stat">
                    <div class="summary-number">~{{ estimated_policies }}</div>
                    <div class="summary-label">Estimated<br>Policies Affected</div>
                </div>
            </div>
        </div>
        
        <h2 style="margin: 30px 0 20px;">Recent Rate Increases</h2>
        <div class="opportunities-grid">
            {% for opp in opportunities %}
            <div class="opportunity-card" data-carrier="{{ opp.competitor|lower }}" data-product="{{ opp.product_lines }}" data-increase="{{ opp.rate_increase }}" data-days="{{ opp.days_until }}">
                <div class="opportunity-header">
                    <div>
                        <div class="competitor-name">{{ opp.competitor }}</div>
                        <div style="color: #6b7280; font-size: 14px; margin-top: 5px;">
                            Effective: {{ opp.effective_date }}
                        </div>
                    </div>
                    <div class="rate-badge">+{{ opp.rate_increase }}%</div>
                </div>
                
                <div class="opportunity-details">
                    <div class="detail-item">
                        <div class="detail-label">Days Until Effective</div>
                        <div class="detail-value">{{ opp.days_until }}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Est. Policies</div>
                        <div class="detail-value">{{ opp.policies_affected }}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Product Lines</div>
                        <div class="detail-value">{{ opp.product_lines }}</div>
                    </div>
                </div>
                <div class="progress-bar"><div class="progress-fill" style="width: {{ opp.urgency_percent }}%; background: {{ opp.urgency_color }};"></div></div>
                <button class="toggle-btn" onclick="toggleDetails(this)">View Details</button>
                <div class="extra-details">
                    <div class="detail-item">Filings: {{ opp.filing_count }}</div>
                    <div class="detail-item">Past Increases:
                        <span class="sparkline" data-points="{{ opp.recent_changes|join(',') }}"></span>
                    </div>
                    <div class="detail-item" style="margin-top:10px; font-size:13px; color:#374151;">
                        {{ opp.call_script }}
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
        
        <div class="timeline">
            <h2>Your Action Calendar</h2>
            <p style="color: #6b7280; margin-bottom: 20px;">Optimal timing for customer outreach based on effective dates</p>
            
            {% for item in timeline %}
            <div class="timeline-item">
                <div class="timeline-content">
                    <div class="timeline-carrier">{{ item.carrier }}</div>
                    <div class="timeline-meta">
                        Rate increase takes effect {{ item.effective_date }}
                        {% if item.days_until <= 30 %}
                        <span class="timeline-badge urgent">{{ item.days_until }} days</span>
                        {% else %}
                        <span class="timeline-badge">{{ item.days_until }} days</span>
                        {% endif %}
                    </div>
                </div>
                <div class="timeline-date">{{ item.outreach_date }}</div>
                <button class="reminder-btn" onclick="saveReminder('{{ item.carrier }}', '{{ item.outreach_date }}')">
                    Save Reminder
                </button>
            </div>
            {% endfor %}
        </div>
        
        <div class="footer">
            <p>
                Generated {{ timestamp }} | Data current as of {{ data_freshness }}<br>
                This report is for {{ agent_carrier }} agents in {{ state }}
            </p>
        </div>
    </div>
    
    <script>
        function saveReminder(carrier, date) {
            const btn = event.target;
            btn.textContent = '✓ Saved';
            btn.classList.add('saved');

            const yyyyMMdd = new Date(date + ' 00:00').toISOString().slice(0,10).replace(/-/g, '');
            const ics = [
                'BEGIN:VCALENDAR',
                'VERSION:2.0',
                'BEGIN:VEVENT',
                'SUMMARY:Reach out to ' + carrier + ' customers',
                'DTSTART;VALUE=DATE:' + yyyyMMdd,
                'DTEND;VALUE=DATE:' + yyyyMMdd,
                'END:VEVENT',
                'END:VCALENDAR'
            ].join('\n');
            const blob = new Blob([ics], {type: 'text/calendar'});
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = carrier.replace(/\\s+/g,'_') + '_' + yyyyMMdd + '.ics';
            link.click();
            setTimeout(() => URL.revokeObjectURL(url), 1000);

            const confirmation = document.createElement('div');
            confirmation.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #10b981; color: white; padding: 16px 24px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); z-index: 1000;';
            confirmation.textContent = `Reminder downloaded for ${carrier}`;
            document.body.appendChild(confirmation);
            setTimeout(() => confirmation.remove(), 3000);
        }

        function toggleDetails(btn) {
            const details = btn.nextElementSibling;
            if (details.style.display === 'block') {
                details.style.display = 'none';
                btn.textContent = 'View Details';
            } else {
                details.style.display = 'block';
                btn.textContent = 'Hide Details';
                renderSparklines();
            }
        }

        function renderSparklines() {
            document.querySelectorAll('.sparkline').forEach(el => {
                if (el.dataset.rendered) return;
                const pts = el.dataset.points.split(',').map(parseFloat);
                if (!pts.length) return;
                const max = Math.max(...pts);
                const min = Math.min(...pts);
                const w = 60, h = 20;
                const step = w / (pts.length - 1);
                let d = '';
                pts.forEach((p,i) => {
                    const x = i * step;
                    const y = h - ((p - min) / (max - min || 1)) * h;
                    d += (i ? 'L':'M') + x + ' ' + y + ' ';
                });
                el.innerHTML = `<svg width="${w}" height="${h}"><path d="${d}" stroke="#3b82f6" stroke-width="2" fill="none"/></svg>`;
                el.dataset.rendered = true;
            });
        }

        function applyFilter() {
            const carrier = document.getElementById('carrierFilter').value.toLowerCase();
            const product = document.getElementById('productFilter').value;
            const sort = document.getElementById('sortFilter').value;
            const cards = Array.from(document.querySelectorAll('.opportunity-card'));
            cards.forEach(card => {
                const cMatch = !carrier || card.dataset.carrier.includes(carrier);
                const pMatch = !product || card.dataset.product.includes(product);
                card.style.display = cMatch && pMatch ? 'block' : 'none';
            });
            if (sort === 'increase') {
                cards.sort((a,b)=>parseFloat(b.dataset.increase)-parseFloat(a.dataset.increase));
            } else {
                cards.sort((a,b)=>parseInt(a.dataset.days)-parseInt(b.dataset.days));
            }
            const grid = document.querySelector('.opportunities-grid');
            cards.forEach(c=>grid.appendChild(c));
        }
    </script>
</body>
</html>
        """
        )

    def get_connection(self):
        return duckdb.connect(self.db_path)

    def generate_agent_report(self, agent_carrier, state, avg_premium=1200):
        """Generate simplified agent report focused on competitor rate increases"""

        conn = self.get_connection()

        # Get competitor rate increases
        opportunities_sql = f"""
        WITH recent_increases AS (
            SELECT 
                Company,
                ROUND(AVG(Premium_Change_Number) * 100, 1) as avg_increase,
                MAX(Effective_Date) as effective_date,
                SUM(Policyholders_Affected_Number) as policies_affected,
                COUNT(*) as filing_count,
                CAST(DATEDIFF('day', CURRENT_DATE, MAX(Effective_Date)) AS INTEGER) as days_until,
                STRING_AGG(DISTINCT Product_Line, ', ') as product_lines,
                ARRAY_SLICE(array_agg(ROUND(Premium_Change_Number * 100, 1) ORDER BY Effective_Date DESC), 1, 3) as recent_changes
            FROM filings
            WHERE State = '{state}'
            AND Company NOT LIKE '%{agent_carrier}%'
            AND Premium_Change_Number > 0.02  -- At least 2% increase
            AND Effective_Date >= CURRENT_DATE - INTERVAL '30 days'
            AND Effective_Date <= CURRENT_DATE + INTERVAL '90 days'
            GROUP BY Company
            HAVING AVG(Premium_Change_Number) > 0.03  -- Average at least 3%
        )
        SELECT *
        FROM recent_increases
        ORDER BY days_until ASC, avg_increase DESC
        LIMIT 15
        """

        opportunities_df = conn.execute(opportunities_sql).fetchdf()

        # Get summary statistics
        summary_sql = f"""
        SELECT 
            COUNT(DISTINCT Company) as companies_raising,
            ROUND(AVG(Premium_Change_Number * 100), 1) as avg_increase,
            ROUND(MAX(Premium_Change_Number * 100), 1) as max_increase,
            SUM(Policyholders_Affected_Number) as total_policies
        FROM filings
        WHERE State = '{state}'
        AND Company NOT LIKE '%{agent_carrier}%'
        AND Premium_Change_Number > 0.02
        AND Effective_Date >= CURRENT_DATE - INTERVAL '30 days'
        AND Effective_Date <= CURRENT_DATE + INTERVAL '90 days'
        """

        summary = conn.execute(summary_sql).fetchone()

        conn.close()

        # Process opportunities
        opportunities = []
        timeline = []
        urgent_count = 0
        product_set = set()

        for _, opp in opportunities_df.iterrows():
            if opp["days_until"] <= 30:
                urgent_count += 1

            # Clean up product lines
            product_lines = opp["product_lines"] if pd.notna(opp["product_lines"]) else "Auto"
            if len(product_lines) > 30:
                product_lines = product_lines[:27] + "..."
            product_set.update([p.strip() for p in product_lines.split(",")])

            urgency_percent = max(0, (90 - opp["days_until"]) / 90 * 100)
            urgency_color = "#dc2626" if opp["days_until"] <= 30 else "#10b981"
            recent_changes = (
                opp["recent_changes"] if isinstance(opp["recent_changes"], list) else []
            )

            opp_data = {
                "competitor": self._clean_company_name(opp["Company"]),
                "rate_increase": round(opp["avg_increase"], 1),
                "effective_date": pd.to_datetime(opp["effective_date"]).strftime("%B %d, %Y"),
                "days_until": int(opp["days_until"]) if opp["days_until"] >= 0 else 0,
                "policies_affected": (
                    f"{int(opp['policies_affected']):,}"
                    if pd.notna(opp["policies_affected"]) and opp["policies_affected"] > 0
                    else "N/A"
                ),
                "product_lines": product_lines,
                "filing_count": int(opp["filing_count"]),
                "recent_changes": recent_changes,
                "urgency_percent": round(urgency_percent, 1),
                "urgency_color": urgency_color,
                "call_script": f"Call former customers when {self._clean_company_name(opp['Company'])} raises rates {round(opp['avg_increase'],1)}%.",
            }
            opportunities.append(opp_data)

            # Add to timeline (calculate outreach date - 30 days before effective)
            if opp["days_until"] >= 0:
                outreach_date = pd.to_datetime(opp["effective_date"]) - timedelta(days=30)
                timeline.append(
                    {
                        "carrier": self._clean_company_name(opp["Company"]),
                        "effective_date": pd.to_datetime(opp["effective_date"]).strftime("%b %d"),
                        "outreach_date": outreach_date.strftime("%b %d"),
                        "days_until": int(opp["days_until"]),
                    }
                )

        # Sort timeline by days until
        timeline.sort(key=lambda x: x["days_until"])

        # Calculate estimated total policies
        total_policies = summary[3] if summary[3] and pd.notna(summary[3]) else 0

        template_data = {
            "agent_carrier": agent_carrier,
            "state": state,
            "report_date": datetime.now().strftime("%B %d, %Y"),
            "total_opportunities": len(opportunities_df),
            "urgent_opportunities": urgent_count if urgent_count > 0 else None,
            "opportunities": opportunities[:8],  # Top 8
            "avg_competitor_increase": round(summary[1], 1) if summary[1] else 0,
            "max_increase": round(summary[2], 1) if summary[2] else 0,
            "estimated_policies": f"{int(total_policies):,}" if total_policies > 0 else "5,000+",
            "timeline": timeline[:6],  # Next 6 actions
            "timestamp": datetime.now().strftime("%I:%M %p"),
            "data_freshness": datetime.now().strftime("%B %d, %Y"),
            "product_options": sorted(product_set),
        }

        return self.report_template.render(**template_data)

    def _clean_company_name(self, name):
        """Clean up company names for display"""
        # Fix spacing issues
        replacements = {
            "InsuranceCompany": " Insurance Company",
            "MutualAutomobile": " Mutual Automobile",
            "andCasualty": " and Casualty",
            "Fire and": "Fire &",
            "Property and": "Property &",
        }

        for old, new in replacements.items():
            name = name.replace(old, new)

        # Shorten if too long
        if len(name) > 35:
            name = name[:32] + "..."

        return name.strip()

    def save_report(self, html_content, filename=None):
        """Save report to file"""
        if filename is None:
            filename = f"rate_increase_intel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        os.makedirs("reports", exist_ok=True)
        filepath = os.path.join("reports", filename)

        with open(filepath, "w") as f:
            f.write(html_content)

        return filepath


# Generate refined reports
if __name__ == "__main__":
    generator = AgentIntelligenceReportV2Refined()

    # Test scenarios
    test_scenarios = [("State Farm", "Arizona"), ("Allstate", "Illinois"), ("Progressive", "Texas")]

    for carrier, state in test_scenarios:
        print(f"\nGenerating refined report for {carrier} agent in {state}...")

        html = generator.generate_agent_report(carrier, state)
        filepath = generator.save_report(
            html,
            f"rate_intel_{carrier.lower().replace(' ', '_')}_{state.lower()}_{datetime.now().strftime('%Y%m%d')}.html",
        )
        print(f"✅ Report saved: {filepath}")

        # Open the first one in browser
        if carrier == "State Farm":
            import webbrowser

            webbrowser.open(f"file://{os.path.abspath(filepath)}")
