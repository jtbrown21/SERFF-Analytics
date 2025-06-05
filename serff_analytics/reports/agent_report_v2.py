# src/agent_report_v2.py
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from jinja2 import Template
import os


class AgentIntelligenceReportV2:
    def __init__(self, db_path="serff_analytics/data/insurance_filings.db"):
        self.db_path = db_path
        self.report_template = Template(
            """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ agent_carrier }} Action Report - {{ state }}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #ffffff;
            color: #111827;
            line-height: 1.5;
        }
        
        /* Hero Section */
        .hero {
            background: linear-gradient(135deg, #1e40af 0%, #7c3aed 100%);
            color: white;
            padding: 40px 20px;
            text-align: center;
        }
        .hero h1 {
            font-size: 32px;
            font-weight: 700;
            margin-bottom: 10px;
        }
        .hero-subtitle {
            font-size: 18px;
            opacity: 0.9;
        }
        
        /* Revenue Impact Banner */
        .revenue-banner {
            background: #fef3c7;
            border: 2px solid #f59e0b;
            margin: 20px;
            padding: 25px;
            border-radius: 12px;
            text-align: center;
        }
        .revenue-amount {
            font-size: 48px;
            font-weight: 800;
            color: #d97706;
            line-height: 1;
        }
        .revenue-subtitle {
            font-size: 18px;
            color: #92400e;
            margin-top: 10px;
        }
        
        /* Action Cards */
        .action-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            padding: 20px;
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .action-card {
            background: white;
            border: 2px solid #e5e7eb;
            border-radius: 12px;
            padding: 25px;
            position: relative;
            transition: all 0.3s;
        }
        .action-card:hover {
            border-color: #3b82f6;
            transform: translateY(-4px);
            box-shadow: 0 10px 20px rgba(0,0,0,0.1);
        }
        
        .urgency-badge {
            position: absolute;
            top: -12px;
            right: 20px;
            background: #ef4444;
            color: white;
            padding: 6px 16px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .competitor-logo {
            width: 60px;
            height: 60px;
            background: #f3f4f6;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
            font-weight: 700;
            color: #6b7280;
            margin-bottom: 15px;
        }
        
        .rate-comparison {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin: 20px 0;
            padding: 15px;
            background: #f9fafb;
            border-radius: 8px;
        }
        
        .rate-them {
            color: #dc2626;
            font-size: 28px;
            font-weight: 700;
        }
        .rate-you {
            color: #059669;
            font-size: 28px;
            font-weight: 700;
        }
        .rate-vs {
            font-size: 18px;
            color: #6b7280;
        }
        
        /* Customer Segments */
        .segment-pills {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin: 15px 0;
        }
        .segment-pill {
            background: #dbeafe;
            color: #1e40af;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 600;
        }
        
        /* Win Probability Meter */
        .win-meter {
            margin: 20px 0;
        }
        .win-meter-bar {
            height: 12px;
            background: #e5e7eb;
            border-radius: 6px;
            overflow: hidden;
            position: relative;
        }
        .win-meter-fill {
            height: 100%;
            background: linear-gradient(90deg, #10b981 0%, #059669 100%);
            transition: width 0.5s;
        }
        .win-meter-label {
            display: flex;
            justify-content: space-between;
            margin-top: 8px;
            font-size: 14px;
        }
        
        /* Action Timeline */
        .timeline-section {
            max-width: 800px;
            margin: 40px auto;
            padding: 0 20px;
        }
        .timeline-header {
            text-align: center;
            margin-bottom: 30px;
        }
        .timeline-header h2 {
            font-size: 28px;
            color: #111827;
            margin-bottom: 10px;
        }
        
        .timeline {
            position: relative;
            padding-left: 40px;
        }
        .timeline::before {
            content: '';
            position: absolute;
            left: 15px;
            top: 0;
            bottom: 0;
            width: 2px;
            background: #e5e7eb;
        }
        
        .timeline-item {
            position: relative;
            margin-bottom: 30px;
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 20px;
        }
        .timeline-item::before {
            content: '';
            position: absolute;
            left: -26px;
            top: 25px;
            width: 12px;
            height: 12px;
            background: #3b82f6;
            border-radius: 50%;
            border: 3px solid white;
            box-shadow: 0 0 0 2px #e5e7eb;
        }
        
        .timeline-item.urgent::before {
            background: #ef4444;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0% { box-shadow: 0 0 0 2px #e5e7eb; }
            50% { box-shadow: 0 0 0 6px rgba(239, 68, 68, 0.2); }
            100% { box-shadow: 0 0 0 2px #e5e7eb; }
        }
        
        /* Quick Stats */
        .stats-bar {
            display: flex;
            justify-content: space-around;
            background: #1e293b;
            color: white;
            padding: 20px;
            flex-wrap: wrap;
            gap: 20px;
        }
        .stat-item {
            text-align: center;
            flex: 1;
            min-width: 150px;
        }
        .stat-number {
            font-size: 32px;
            font-weight: 700;
            color: #fbbf24;
        }
        .stat-label {
            font-size: 14px;
            opacity: 0.8;
            margin-top: 5px;
        }
        
        /* Call to Action */
        .cta-section {
            background: #f0fdf4;
            border: 2px solid #10b981;
            border-radius: 12px;
            padding: 30px;
            margin: 40px 20px;
            text-align: center;
        }
        .cta-section h3 {
            color: #059669;
            font-size: 24px;
            margin-bottom: 15px;
        }
        .cta-button {
            display: inline-block;
            background: #10b981;
            color: white;
            padding: 12px 30px;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 600;
            margin-top: 15px;
            transition: background 0.3s;
        }
        .cta-button:hover {
            background: #059669;
        }
        
        /* Mobile Responsive */
        @media (max-width: 768px) {
            .hero h1 { font-size: 24px; }
            .revenue-amount { font-size: 36px; }
            .action-grid { grid-template-columns: 1fr; }
            .rate-them, .rate-you { font-size: 24px; }
        }
        
        /* Print Styles */
        @media print {
            body { background: white; }
            .hero { background: none; color: black; border-bottom: 2px solid black; }
            .action-card { page-break-inside: avoid; }
            .cta-section { display: none; }
        }
    </style>
</head>
<body>
    <!-- Hero Section -->
    <div class="hero">
        <h1>{{ total_opportunities }} Win-Back Opportunities Found!</h1>
        <div class="hero-subtitle">{{ agent_carrier }} Agent • {{ state }} • Updated {{ report_time }}</div>
    </div>
    
    <!-- Revenue Impact Banner -->
    <div class="revenue-banner">
        <div class="revenue-amount">${{ potential_revenue }}</div>
        <div class="revenue-subtitle">Potential Commission Revenue Available</div>
        <div style="margin-top: 10px; font-size: 14px; color: #78350f;">
            Based on {{ total_policies }} affected policies at ${{ avg_premium }} average premium
        </div>
    </div>
    
    <!-- Quick Stats Bar -->
    <div class="stats-bar">
        <div class="stat-item">
            <div class="stat-number">{{ your_advantage }}%</div>
            <div class="stat-label">Average Price Advantage</div>
        </div>
        <div class="stat-item">
            <div class="stat-number">{{ urgent_count }}</div>
            <div class="stat-label">Act Within 30 Days</div>
        </div>
        <div class="stat-item">
            <div class="stat-number">#{{ market_position }}</div>
            <div class="stat-label">Your Market Position</div>
        </div>
        <div class="stat-item">
            <div class="stat-number">{{ win_rate }}%</div>
            <div class="stat-label">Expected Win Rate</div>
        </div>
    </div>
    
    <!-- Action Cards -->
    <div style="padding: 20px; max-width: 1200px; margin: 0 auto;">
        <h2 style="font-size: 28px; margin-bottom: 20px; text-align: center;">
            🎯 Your Top Win-Back Targets
        </h2>
    </div>
    
    <div class="action-grid">
        {% for opp in top_opportunities %}
        <div class="action-card">
            {% if opp.days_until <= 30 %}
            <div class="urgency-badge">Act Now</div>
            {% endif %}
            
            <div class="competitor-logo">{{ opp.logo }}</div>
            <h3 style="font-size: 22px; margin-bottom: 10px;">{{ opp.competitor }}</h3>
            
            <div class="rate-comparison">
                <div>
                    <div class="rate-them">+{{ opp.their_rate }}%</div>
                    <div style="font-size: 12px; color: #6b7280;">Their increase</div>
                </div>
                <div class="rate-vs">VS</div>
                <div>
                    <div class="rate-you">{{ your_rate_display }}</div>
                    <div style="font-size: 12px; color: #6b7280;">Your rate</div>
                </div>
            </div>
            
            <div class="win-meter">
                <div style="font-weight: 600; margin-bottom: 5px;">Win Probability</div>
                <div class="win-meter-bar">
                    <div class="win-meter-fill" style="width: {{ opp.win_probability }}%"></div>
                </div>
                <div class="win-meter-label">
                    <span>Low</span>
                    <span style="font-weight: 700; color: #059669;">{{ opp.win_probability }}%</span>
                    <span>High</span>
                </div>
            </div>
            
            <div style="margin-top: 20px;">
                <strong>Best Targets:</strong>
                <div class="segment-pills">
                    {% for segment in opp.segments %}
                    <div class="segment-pill">{{ segment }}</div>
                    {% endfor %}
                </div>
            </div>
            
            <div style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #e5e7eb;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                    <span style="color: #6b7280;">Effective Date:</span>
                    <strong>{{ opp.effective_date }}</strong>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                    <span style="color: #6b7280;">Est. Policies:</span>
                    <strong>{{ opp.policies_affected }}</strong>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #6b7280;">Commission Opp:</span>
                    <strong style="color: #059669;">${{ opp.commission_opportunity }}</strong>
                </div>
            </div>
        </div>
        {% endfor %}
    </div>
    
    <!-- Action Timeline -->
    <div class="timeline-section">
        <div class="timeline-header">
            <h2>📅 Your Action Calendar</h2>
            <p style="color: #6b7280;">When to reach out for maximum impact</p>
        </div>
        
        <div class="timeline">
            {% for event in timeline %}
            <div class="timeline-item {% if event.days_until <= 30 %}urgent{% endif %}">
                <div style="display: flex; justify-content: space-between; align-items: start;">
                    <div>
                        <h4 style="font-size: 18px; margin-bottom: 5px;">{{ event.carrier }}</h4>
                        <div style="color: #6b7280; font-size: 14px;">
                            Rate increase effective {{ event.date }}
                        </div>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-size: 24px; font-weight: 700; color: #3b82f6;">
                            {{ event.days_until }}
                        </div>
                        <div style="font-size: 12px; color: #6b7280;">days</div>
                    </div>
                </div>
                <div style="margin-top: 15px; padding: 10px; background: #f3f4f6; border-radius: 6px;">
                    <strong>Action Window:</strong> {{ event.action_window }}
                </div>
            </div>
            {% endfor %}
        </div>
    </div>
    
    <!-- Call to Action -->
    <div class="cta-section">
        <h3>Ready to Win Back Customers?</h3>
        <p>Download your call list and talking points to start converting these opportunities today.</p>
        <a href="#" class="cta-button">Download Action Kit</a>
    </div>
    
    <!-- Footer -->
    <div style="text-align: center; padding: 20px; color: #6b7280; font-size: 12px;">
        Generated {{ timestamp }} • Data current as of {{ data_date }} • Next update: {{ next_update }}
    </div>
</body>
</html>
        """
        )

    def get_connection(self):
        return duckdb.connect(self.db_path)

    def generate_agent_report(self, agent_carrier, state, avg_premium=1200, commission_rate=0.15):
        """Generate enhanced agent report with revenue focus"""

        conn = self.get_connection()

        # Find exact carrier name
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
        SELECT 
            AVG(Premium_Change_Number) * 100 as avg_rate,
            COUNT(*) as filing_count
        FROM filings
        WHERE Company = '{exact_carrier}'
        AND State = '{state}'
        AND Effective_Date >= CURRENT_DATE - INTERVAL '6 months'
        """
        agent_rate_result = conn.execute(agent_rate_sql).fetchone()
        agent_rate = agent_rate_result[0] if agent_rate_result[0] else 0

        # Get opportunities with enhanced data
        opportunities_sql = f"""
        WITH competitor_rates AS (
            SELECT 
                Company,
                ROUND(AVG(Premium_Change_Number) * 100, 1) as avg_increase,
                MAX(Effective_Date) as effective_date,
                SUM(Policyholders_Affected_Number) as policies_affected,
                COUNT(*) as filing_count,
                -- Calculate days until effective (fixed for DuckDB)
                CAST(DATEDIFF('day', CURRENT_DATE, MAX(Effective_Date)) AS INTEGER) as days_until
            FROM filings
            WHERE State = '{state}'
            AND Company != '{exact_carrier}'
            AND Premium_Change_Number > 0.02  -- At least 2% increase
            AND Effective_Date >= CURRENT_DATE
            AND Effective_Date <= CURRENT_DATE + INTERVAL '90 days'
            GROUP BY Company
        )
        SELECT 
            *,
            avg_increase - {agent_rate} as rate_advantage,
            -- Calculate win probability based on rate difference
            CASE 
                WHEN avg_increase - {agent_rate} > 10 THEN 85
                WHEN avg_increase - {agent_rate} > 7 THEN 75
                WHEN avg_increase - {agent_rate} > 5 THEN 65
                WHEN avg_increase - {agent_rate} > 3 THEN 55
                ELSE 45
            END as win_probability
        FROM competitor_rates
        WHERE avg_increase > {agent_rate} + 2  -- At least 2% higher
        ORDER BY rate_advantage DESC, policies_affected DESC
        LIMIT 12
        """

        opportunities_df = conn.execute(opportunities_sql).fetchdf()

        # Get market position
        position_sql = f"""
        WITH carrier_rates AS (
            SELECT 
                Company,
                AVG(Premium_Change_Number) * 100 as avg_rate,
                ROW_NUMBER() OVER (ORDER BY AVG(Premium_Change_Number) ASC) as position
            FROM filings
            WHERE State = '{state}'
            AND Effective_Date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY Company
        )
        SELECT position
        FROM carrier_rates
        WHERE Company = '{exact_carrier}'
        """
        position_result = conn.execute(position_sql).fetchone()
        market_position = position_result[0] if position_result else "N/A"

        conn.close()

        # Process opportunities
        top_opportunities = []
        timeline = []
        total_commission_opp = 0
        total_policies = 0

        for _, opp in opportunities_df.iterrows():
            policies = int(opp["policies_affected"]) if pd.notna(opp["policies_affected"]) else 100
            commission_opp = int(
                policies * avg_premium * commission_rate * (opp["win_probability"] / 100)
            )
            total_commission_opp += commission_opp
            total_policies += policies

            opp_data = {
                "competitor": self._clean_company_name(opp["Company"]),
                "logo": self._get_company_logo(opp["Company"]),
                "their_rate": round(opp["avg_increase"], 1),
                "rate_advantage": round(opp["rate_advantage"], 1),
                "effective_date": pd.to_datetime(opp["effective_date"]).strftime("%b %d"),
                "days_until": int(opp["days_until"]),
                "win_probability": int(opp["win_probability"]),
                "policies_affected": f"{policies:,}",
                "commission_opportunity": f"{commission_opp:,}",
                "segments": self._get_target_segments(opp["Company"], opp["avg_increase"]),
            }
            top_opportunities.append(opp_data)

            # Add to timeline
            if opp["days_until"] <= 60:
                timeline.append(
                    {
                        "carrier": self._clean_company_name(opp["Company"]),
                        "date": pd.to_datetime(opp["effective_date"]).strftime("%B %d"),
                        "days_until": int(opp["days_until"]),
                        "action_window": self._get_action_window(opp["days_until"]),
                    }
                )

        # Sort timeline
        timeline.sort(key=lambda x: x["days_until"])

        # Calculate summary metrics
        avg_advantage = (
            round(opportunities_df["rate_advantage"].mean(), 1) if len(opportunities_df) > 0 else 0
        )
        urgent_count = len([o for o in top_opportunities if o["days_until"] <= 30])

        template_data = {
            "agent_carrier": agent_carrier,
            "state": state,
            "report_time": datetime.now().strftime("%I:%M %p"),
            "total_opportunities": len(opportunities_df),
            "potential_revenue": f"{total_commission_opp:,}",
            "total_policies": f"{total_policies:,}",
            "avg_premium": f"{avg_premium:,}",
            "your_advantage": avg_advantage,
            "urgent_count": urgent_count,
            "market_position": market_position,
            "win_rate": (
                int(opportunities_df["win_probability"].mean()) if len(opportunities_df) > 0 else 0
            ),
            "your_rate_display": (
                f"+{round(agent_rate, 1)}%" if agent_rate > 0 else f"{round(agent_rate, 1)}%"
            ),
            "top_opportunities": top_opportunities[:6],  # Show top 6
            "timeline": timeline[:5],  # Next 5 events
            "timestamp": datetime.now().strftime("%Y-%m-%d %I:%M %p"),
            "data_date": datetime.now().strftime("%B %d, %Y"),
            "next_update": (datetime.now() + timedelta(days=7)).strftime("%B %d"),
        }

        return self.report_template.render(**template_data)

    def _clean_company_name(self, name):
        """Clean and shorten company names"""
        # Common replacements
        replacements = {
            "InsuranceCompany": " Insurance",
            "MutualAutomobile": " Mutual Auto",
            "andCasualty": " & Casualty",
            "InsuranceCompany": " Insurance",
            "Fire and": "Fire &",
        }

        for old, new in replacements.items():
            name = name.replace(old, new)

        # Shorten long names
        if "State Farm" in name:
            return "State Farm"
        elif "Progressive" in name:
            return "Progressive"
        elif "Allstate" in name:
            return "Allstate"
        elif "Geico" in name or "GEICO" in name:
            return "GEICO"
        elif "Farmers" in name:
            return "Farmers"
        elif "Liberty Mutual" in name:
            return "Liberty Mutual"

        # Truncate if still too long
        if len(name) > 25:
            name = name[:22] + "..."

        return name.strip()

    def _get_company_logo(self, company):
        """Get logo/initial for company"""
        name = self._clean_company_name(company)

        # Return first 2-3 characters
        if "State Farm" in name:
            return "SF"
        elif "Progressive" in name:
            return "PG"
        elif "Allstate" in name:
            return "AS"
        elif "GEICO" in name:
            return "GE"
        elif "Farmers" in name:
            return "FM"
        elif "Liberty" in name:
            return "LM"
        else:
            # Return first 2 letters
            words = name.split()
            if len(words) >= 2:
                return words[0][0] + words[1][0]
            else:
                return name[:2].upper()

    def _get_target_segments(self, company, rate_increase):
        """Determine best customer segments to target"""
        segments = []

        # Base segments
        if rate_increase > 10:
            segments.append("Multi-car households")
            segments.append("Long-term customers")
        if rate_increase > 7:
            segments.append("Good drivers")
            segments.append("Bundle opportunities")
        if rate_increase > 5:
            segments.append("Price-sensitive")

        # Company-specific
        if "State Farm" in company:
            segments.append("Loyalty seekers")
        elif "Progressive" in company:
            segments.append("Tech-savvy")
        elif "Allstate" in company:
            segments.append("Full coverage")

        return segments[:3]  # Top 3

    def _get_action_window(self, days_until):
        """Get recommended action window"""
        if days_until <= 30:
            return "Contact immediately - renewal notices going out"
        elif days_until <= 45:
            return "Prime time - customers shopping for alternatives"
        elif days_until <= 60:
            return "Early outreach - build awareness"
        else:
            return "Plan campaign - prepare materials"

    def save_report(self, html_content, filename=None):
        """Save report to file"""
        if filename is None:
            filename = f"agent_action_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        os.makedirs("reports", exist_ok=True)
        filepath = os.path.join("reports", filename)

        with open(filepath, "w") as f:
            f.write(html_content)

        return filepath


# Generate V2 reports
if __name__ == "__main__":
    generator = AgentIntelligenceReportV2()

    # Test scenarios
    test_scenarios = [
        ("State Farm", "Arizona"),
        ("Allstate", "Illinois"),
    ]

    for carrier, state in test_scenarios:
        print(f"\nGenerating V2 report for {carrier} agent in {state}...")

        html = generator.generate_agent_report(carrier, state)
        if html:
            filepath = generator.save_report(
                html,
                f"agent_action_v2_{carrier.lower().replace(' ', '_')}_{state.lower()}_{datetime.now().strftime('%Y%m%d')}.html",
            )
            print(f"✅ V2 Report saved: {filepath}")

            # Open the first one
            if carrier == "State Farm":
                import webbrowser

                webbrowser.open(f"file://{os.path.abspath(filepath)}")
        else:
            print(f"❌ No data found for {carrier} in {state}")
