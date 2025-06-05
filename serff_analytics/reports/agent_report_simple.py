# Create a new file: src/agent_report_v3_simple.py
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import os
import base64
from io import BytesIO
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


class AgentIntelligenceReportV3Simple:
    def __init__(self, db_path="serff_analytics/data/insurance_filings.db"):
        self.db_path = db_path
        self.html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ agent_carrier }} Intelligence Report</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;900&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f9fafb;
            color: #1f2937;
            line-height: 1.6;
        }
        
        /* Container */
        .container {
            max-width: 800px;
            margin: 0 auto;
            background: white;
        }
        
        /* Header */
        .header {
            text-align: center;
            padding: 24px 32px;
            border-bottom: 1px solid #e5e7eb;
        }
        .header-meta {
            font-size: 13px;
            color: #6b7280;
        }
        
        /* Hero Metrics */
        .hero-metrics {
            display: flex;
            gap: 16px;
            padding: 32px;
            background: white;
        }
        .metric-card {
            flex: 1;
            text-align: center;
            padding: 24px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        }
        .metric-value {
            font-size: 48px;
            font-weight: 900;
            line-height: 1;
            color: #111827;
            margin-bottom: 8px;
        }
        .metric-value.teal {
            color: #14b8a6;
        }
        .metric-label {
            font-size: 14px;
            font-weight: 600;
            color: #6b7280;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        /* Market Pulse */
        .market-pulse {
            padding: 32px;
            background: #ffffff;
        }
        .section-title {
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 16px;
            color: #111827;
        }
        .sparkline-container {
            margin: 24px 0;
            text-align: center;
        }
        .sparkline-caption {
            font-size: 14px;
            color: #6b7280;
            margin-top: 8px;
        }
        .sparkline-caption .position {
            color: #14b8a6;
            font-weight: 700;
        }
        
        /* Opportunities */
        .opportunities {
            padding: 32px;
            background: #f3f4f6;
        }
        .opp-card {
            background: white;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 16px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            transition: all 0.3s ease;
        }
        .opp-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        }
        .opp-header {
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 16px;
        }
        .opp-carrier {
            font-size: 20px;
            font-weight: 700;
            color: #111827;
        }
        .opp-meta {
            font-size: 14px;
            color: #6b7280;
            margin-top: 4px;
        }
        .opp-rate {
            text-align: right;
        }
        .opp-rate-value {
            font-size: 32px;
            font-weight: 900;
            color: #dc2626;
        }
        .opp-rate-label {
            font-size: 12px;
            color: #6b7280;
        }
        .opp-details {
            border-top: 1px solid #e5e7eb;
            margin-top: 16px;
            padding-top: 16px;
        }
        .opp-stats {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
        }
        .opp-stat-label {
            font-size: 12px;
            color: #6b7280;
            text-transform: uppercase;
        }
        .opp-stat-value {
            font-size: 24px;
            font-weight: 700;
            margin-top: 4px;
        }
        .opp-stat-value.green {
            color: #059669;
        }
        .opp-stat-value.teal {
            color: #14b8a6;
        }
        
        /* Timeline */
        .timeline {
            padding: 32px;
            background: white;
        }
        .timeline-table {
            width: 100%;
            border-collapse: collapse;
        }
        .timeline-table th {
            text-align: left;
            padding: 12px;
            font-weight: 600;
            color: #6b7280;
            font-size: 12px;
            text-transform: uppercase;
            border-bottom: 2px solid #e5e7eb;
        }
        .timeline-table td {
            padding: 16px 12px;
            border-bottom: 1px solid #f3f4f6;
        }
        .timeline-urgent {
            color: #dc2626;
            font-weight: 700;
        }
        
        /* CTA */
        .cta {
            background: #f0fdf4;
            padding: 48px 32px;
            text-align: center;
        }
        .cta-title {
            font-size: 28px;
            font-weight: 700;
            color: #111827;
            margin-bottom: 16px;
        }
        .cta-subtitle {
            color: #6b7280;
            margin-bottom: 24px;
        }
        .cta-button {
            display: inline-block;
            background: linear-gradient(135deg, #14b8a6 0%, #0d9488 100%);
            color: white;
            padding: 12px 32px;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 600;
            font-size: 16px;
            transition: all 0.3s ease;
        }
        .cta-button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(20, 184, 166, 0.3);
        }
        
        /* Mobile */
        @media (max-width: 640px) {
            .hero-metrics {
                flex-direction: column;
            }
            .metric-value {
                font-size: 36px;
            }
            .opp-stats {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <div class="header-meta">
                {{ report_date }} • {{ agent_carrier }} • {{ state }}
            </div>
        </div>
        
        <!-- Hero Metrics -->
        <div class="hero-metrics">
            <div class="metric-card">
                <div class="metric-value teal">${{ revenue_opportunity }}</div>
                <div class="metric-label">Revenue Opportunity</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ win_back_count }}</div>
                <div class="metric-label">Win-Back Targets</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ urgent_actions }}</div>
                <div class="metric-label">Act This Week</div>
            </div>
        </div>
        
        <!-- Market Pulse -->
        <div class="market-pulse">
            <h2 class="section-title">Market Pulse</h2>
            <div class="sparkline-container">
                <img src="{{ sparkline_src }}" alt="Market trend" style="max-width: 100%; height: auto;">
                <div class="sparkline-caption">
                    Rate filing activity over the last 90 days • Your position: 
                    <span class="position">#{{ market_position }}</span> of {{ total_carriers }}
                </div>
            </div>
        </div>
        
        <!-- Opportunities -->
        <div class="opportunities">
            <h2 class="section-title">Top Opportunities</h2>
            {% for opp in opportunities %}
            <div class="opp-card">
                <div class="opp-header">
                    <div>
                        <div class="opp-carrier">{{ opp.carrier }}</div>
                        <div class="opp-meta">Effective {{ opp.effective_date }} • {{ opp.days_until }} days</div>
                    </div>
                    <div class="opp-rate">
                        <div class="opp-rate-value">+{{ opp.rate }}%</div>
                        <div class="opp-rate-label">their increase</div>
                    </div>
                </div>
                <div class="opp-details">
                    <div class="opp-stats">
                        <div>
                            <div class="opp-stat-label">Your Advantage</div>
                            <div class="opp-stat-value green">{{ opp.advantage }}%</div>
                        </div>
                        <div>
                            <div class="opp-stat-label">Commission Opp</div>
                            <div class="opp-stat-value teal">${{ opp.commission }}</div>
                        </div>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
        
        <!-- Timeline -->
        <div class="timeline">
            <h2 class="section-title">Action Timeline</h2>
            <table class="timeline-table">
                <thead>
                    <tr>
                        <th>When</th>
                        <th>Carrier</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
                    {% for event in timeline %}
                    <tr>
                        <td class="{% if event.urgent %}timeline-urgent{% endif %}">
                            {{ event.date }}
                        </td>
                        <td>{{ event.carrier }}</td>
                        <td>{{ event.action }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        
        <!-- CTA -->
        <div class="cta">
            <h2 class="cta-title">Ready to capture ${{ revenue_opportunity }}?</h2>
            <p class="cta-subtitle">
                Access your personalized dashboard for real-time updates and downloadable call lists.
            </p>
            <a href="{{ dashboard_url }}" class="cta-button">
                Explore Live Dashboard →
            </a>
        </div>
    </div>
</body>
</html>
        """

    def get_connection(self):
        return duckdb.connect(self.db_path)

    def generate_sparkline(self, data, filename="sparkline.png"):
        """Generate sparkline image"""
        fig, ax = plt.subplots(figsize=(8, 2), facecolor="None")

        x = range(len(data))
        ax.plot(x, data, "#14b8a6", linewidth=3, solid_capstyle="round")
        ax.fill_between(x, data, alpha=0.3, color="#14b8a6")

        # Style
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])

        # Add dots
        ax.scatter([0, len(data) - 1], [data[0], data[-1]], color="#14b8a6", s=50, zorder=5)

        # Save
        os.makedirs("reports/assets", exist_ok=True)
        filepath = f"reports/assets/{filename}"
        plt.savefig(filepath, transparent=True, bbox_inches="tight", pad_inches=0.1, dpi=200)
        plt.close()

        # Convert to base64 for embedding
        with open(filepath, "rb") as f:
            img_data = f.read()
        b64_data = base64.b64encode(img_data).decode()

        return f"data:image/png;base64,{b64_data}"

    def generate_agent_report(
        self, agent_carrier, state, dashboard_url="https://app.insureintell.com/dashboard"
    ):
        """Generate clean HTML report"""

        conn = self.get_connection()

        # Get carrier data
        carrier_search = f"""
        SELECT DISTINCT Company 
        FROM filings 
        WHERE Company LIKE '%{agent_carrier}%'
        AND State = '{state}'
        LIMIT 1
        """
        carrier_result = conn.execute(carrier_search).fetchone()
        if not carrier_result:
            conn.close()
            return None

        exact_carrier = carrier_result[0]

        # Get agent's rate
        agent_rate_sql = f"""
        SELECT AVG(Premium_Change_Number) * 100 as avg_rate
        FROM filings
        WHERE Company = '{exact_carrier}'
        AND State = '{state}'
        AND Effective_Date >= CURRENT_DATE - INTERVAL '6 months'
        """
        agent_rate_result = conn.execute(agent_rate_sql).fetchone()
        agent_rate = agent_rate_result[0] if agent_rate_result[0] else 0

        # Get opportunities
        opportunities_sql = f"""
        WITH competitor_rates AS (
            SELECT 
                Company,
                ROUND(AVG(Premium_Change_Number) * 100, 1) as avg_increase,
                MAX(Effective_Date) as effective_date,
                SUM(Policyholders_Affected_Number) as policies_affected,
                CAST(DATEDIFF('day', CURRENT_DATE, MAX(Effective_Date)) AS INTEGER) as days_until
            FROM filings
            WHERE State = '{state}'
            AND Company != '{exact_carrier}'
            AND Premium_Change_Number > 0.03
            AND Effective_Date >= CURRENT_DATE
            AND Effective_Date <= CURRENT_DATE + INTERVAL '60 days'
            GROUP BY Company
        )
        SELECT *,
            avg_increase - {agent_rate} as rate_advantage
        FROM competitor_rates
        WHERE avg_increase > {agent_rate} + 2
        ORDER BY days_until ASC, rate_advantage DESC
        LIMIT 6
        """

        opportunities_df = conn.execute(opportunities_sql).fetchdf()

        # Get market trend data
        trend_sql = f"""
        SELECT 
            DATE_TRUNC('week', Effective_Date) as week,
            COUNT(*) as filings
        FROM filings
        WHERE State = '{state}'
        AND Effective_Date >= CURRENT_DATE - INTERVAL '90 days'
        AND Effective_Date <= CURRENT_DATE
        GROUP BY week
        ORDER BY week
        """

        trend_df = conn.execute(trend_sql).fetchdf()

        # Get market position
        position_sql = f"""
        WITH carrier_rates AS (
            SELECT 
                Company,
                ROW_NUMBER() OVER (ORDER BY AVG(Premium_Change_Number) ASC) as position,
                COUNT(*) OVER () as total
            FROM filings
            WHERE State = '{state}'
            AND Effective_Date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY Company
        )
        SELECT position, total
        FROM carrier_rates
        WHERE Company = '{exact_carrier}'
        """
        position_result = conn.execute(position_sql).fetchone()
        market_position = position_result[0] if position_result else 1
        total_carriers = position_result[1] if position_result else 1

        conn.close()

        # Generate sparkline
        sparkline_data = trend_df["filings"].tolist() if len(trend_df) > 0 else [5, 8, 12, 10, 15]
        sparkline_src = self.generate_sparkline(sparkline_data)

        # Process opportunities
        opportunities = []
        timeline = []
        total_revenue = 0
        urgent_count = 0

        for _, opp in opportunities_df.iterrows():
            policies = int(opp["policies_affected"]) if pd.notna(opp["policies_affected"]) else 100
            commission = int(policies * 1200 * 0.15 * 0.65)
            total_revenue += commission

            if opp["days_until"] <= 7:
                urgent_count += 1

            opportunities.append(
                {
                    "carrier": self._clean_name(opp["Company"]),
                    "rate": round(opp["avg_increase"], 1),
                    "advantage": round(opp["rate_advantage"], 1),
                    "effective_date": pd.to_datetime(opp["effective_date"]).strftime("%b %d"),
                    "days_until": int(opp["days_until"]),
                    "commission": f"{commission:,}",
                }
            )

            if opp["days_until"] <= 30:
                timeline.append(
                    {
                        "date": pd.to_datetime(opp["effective_date"]).strftime("%b %d"),
                        "carrier": self._clean_name(opp["Company"]),
                        "action": "Begin outreach campaign",
                        "urgent": opp["days_until"] <= 7,
                    }
                )

        # Build template data
        template_data = {
            "report_date": datetime.now().strftime("%B %d, %Y"),
            "agent_carrier": agent_carrier,
            "state": state,
            "revenue_opportunity": f"{total_revenue:,}",
            "win_back_count": len(opportunities_df),
            "urgent_actions": urgent_count,
            "sparkline_src": sparkline_src,
            "market_position": market_position,
            "total_carriers": total_carriers,
            "opportunities": opportunities[:4],
            "timeline": timeline[:5],
            "dashboard_url": f"{dashboard_url}?agent={agent_carrier.lower()}&state={state.lower()}",
        }

        # Simple template rendering
        html = self.html_template
        for key, value in template_data.items():
            if isinstance(value, list):
                # Handle lists specially
                if key == "opportunities":
                    opp_html = ""
                    for opp in value:
                        opp_html += f"""
                        <div class="opp-card">
                            <div class="opp-header">
                                <div>
                                    <div class="opp-carrier">{opp['carrier']}</div>
                                    <div class="opp-meta">Effective {opp['effective_date']} • {opp['days_until']} days</div>
                                </div>
                                <div class="opp-rate">
                                    <div class="opp-rate-value">+{opp['rate']}%</div>
                                    <div class="opp-rate-label">their increase</div>
                                </div>
                            </div>
                            <div class="opp-details">
                                <div class="opp-stats">
                                    <div>
                                        <div class="opp-stat-label">Your Advantage</div>
                                        <div class="opp-stat-value green">{opp['advantage']}%</div>
                                    </div>
                                    <div>
                                        <div class="opp-stat-label">Commission Opp</div>
                                        <div class="opp-stat-value teal">${opp['commission']}</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        """
                    html = html.replace("{% for opp in opportunities %}...{% endfor %}", opp_html)
                elif key == "timeline":
                    timeline_html = ""
                    for event in value:
                        timeline_html += f"""
                        <tr>
                            <td class="{'timeline-urgent' if event.get('urgent') else ''}">
                                {event['date']}
                            </td>
                            <td>{event['carrier']}</td>
                            <td>{event['action']}</td>
                        </tr>
                        """
                    html = html.replace("{% for event in timeline %}...{% endfor %}", timeline_html)
            else:
                html = html.replace("{{ " + key + " }}", str(value))

        # Clean up template syntax
        import re

        html = re.sub(r"{%.*?%}", "", html)

        return html

    def _clean_name(self, name):
        """Clean company names"""
        name = name.replace("InsuranceCompany", " Insurance")
        name = name.replace("MutualAutomobile", " Mutual")
        if len(name) > 25:
            name = name[:22] + "..."
        return name.strip()

    def save_report(self, html_content, filename=None):
        """Save report"""
        if filename is None:
            filename = f"agent_intel_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        os.makedirs("reports", exist_ok=True)
        filepath = os.path.join("reports", filename)

        with open(filepath, "w") as f:
            f.write(html_content)

        return filepath


# Generate reports
if __name__ == "__main__":
    generator = AgentIntelligenceReportV3Simple()

    carrier = "State Farm"
    state = "Arizona"

    print(f"\nGenerating V3 Simple report for {carrier} agent in {state}...")

    html = generator.generate_agent_report(carrier, state)
    if html:
        filepath = generator.save_report(
            html,
            f"agent_intel_v3_{carrier.lower().replace(' ', '_')}_{state.lower()}_{datetime.now().strftime('%Y%m%d')}.html",
        )
        print(f"✅ Report saved: {filepath}")

        import webbrowser

        webbrowser.open(f"file://{os.path.abspath(filepath)}")
    else:
        print(f"❌ No data found for {carrier} in {state}")
