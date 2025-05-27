import duckdb
import pandas as pd
from datetime import datetime, timedelta
from jinja2 import Template
import os

class AgentIntelligenceReport:
    def __init__(self, db_path='data/insurance_filings.db'):
        self.db_path = db_path
        self.report_template = Template("""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ agent_carrier }} - {{ state }} Competitive Intelligence</title>
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
            background: #fbbf24;
            color: #78350f;
            padding: 4px 12px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 14px;
        }
        .rate-comparison {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            margin: 20px 0;
            padding: 20px;
            background: #f9fafb;
            border-radius: 8px;
        }
        .rate-item {
            text-align: center;
        }
        .rate-label {
            font-size: 12px;
            color: #6b7280;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .rate-value {
            font-size: 24px;
            font-weight: 700;
            margin-top: 5px;
        }
        .their-rate { color: #dc2626; }
        .your-rate { color: #059669; }
        .advantage { color: #1e3a8a; }
        
        /* Impact Meter */
        .impact-meter {
            margin-top: 15px;
            padding: 15px;
            background: #eff6ff;
            border-radius: 8px;
        }
        .impact-label {
            font-size: 12px;
            color: #6b7280;
            margin-bottom: 5px;
        }
        .impact-bar {
            height: 8px;
            background: #e5e7eb;
            border-radius: 4px;
            overflow: hidden;
        }
        .impact-fill {
            height: 100%;
            background: linear-gradient(90deg, #3b82f6 0%, #1e40af 100%);
            transition: width 0.3s;
        }
        .impact-details {
            display: flex;
            justify-content: space-between;
            margin-top: 8px;
            font-size: 14px;
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
        
        /* Market Context */
        .market-context {
            background: #f3f4f6;
            border-radius: 8px;
            padding: 20px;
            margin-top: 20px;
        }
        .context-title {
            font-weight: 600;
            margin-bottom: 10px;
            color: #374151;
        }
        .context-item {
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #e5e7eb;
        }
        .context-item:last-child {
            border-bottom: none;
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
            gap: 20px;
            margin-bottom: 20px;
            padding-bottom: 20px;
            border-bottom: 1px solid #e5e7eb;
        }
        .timeline-date {
            min-width: 100px;
            font-weight: 600;
            color: #6b7280;
        }
        .timeline-content {
            flex: 1;
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
        
        @media (max-width: 768px) {
            .rate-comparison {
                grid-template-columns: 1fr;
            }
            .summary-grid {
                grid-template-columns: 1fr;
            }
        }
        
        @media print {
            body { background: white; }
            .container { max-width: 100%; }
            .opportunity-card { break-inside: avoid; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{{ agent_carrier }} Competitive Intelligence</h1>
            <div class="header-meta">
                <span>📍 {{ state }}</span>
                <span>📅 {{ report_date }}</span>
                <span>🔄 {{ total_filings }} new filings analyzed</span>
            </div>
        </div>
        
        {% if urgent_opportunities %}
        <div class="alert-box">
            <span class="alert-icon">⚡</span>
            <div>
                <strong>Action Required:</strong> {{ urgent_opportunities|length }} competitors with major rate increases taking effect soon. 
                These represent your best immediate winback opportunities.
            </div>
        </div>
        {% endif %}
        
        <div class="summary-section">
            <h2>Market Position Summary</h2>
            <div class="summary-grid">
                <div class="summary-stat">
                    <div class="summary-number">{{ your_position }}</div>
                    <div class="summary-label">Your Rate Position<br>(out of {{ total_carriers }} carriers)</div>
                </div>
                <div class="summary-stat">
                    <div class="summary-number">{{ winback_count }}</div>
                    <div class="summary-label">Winback<br>Opportunities</div>
                </div>
                <div class="summary-stat">
                    <div class="summary-number">{{ avg_competitor_increase }}%</div>
                    <div class="summary-label">Avg Competitor<br>Increase</div>
                </div>
                <div class="summary-stat">
                    <div class="summary-number">{{ your_rate_change }}%</div>
                    <div class="summary-label">Your Rate<br>Change</div>
                </div>
            </div>
            
            <div class="market-context">
                <div class="context-title">Market Context</div>
                <div class="context-item">
                    <span>Market Direction</span>
                    <strong>{{ market_direction }}</strong>
                </div>
                <div class="context-item">
                    <span>Most Aggressive Competitor</span>
                    <strong>{{ most_aggressive }}</strong>
                </div>
                <div class="context-item">
                    <span>Total Policies Affected</span>
                    <strong>{{ total_policies_affected|default('N/A') }}</strong>
                </div>
            </div>
        </div>
        
        <h2 style="margin: 30px 0 20px;">Top Winback Opportunities</h2>
        <div class="opportunities-grid">
            {% for opp in opportunities %}
            <div class="opportunity-card">
                <div class="opportunity-header">
                    <div>
                        <div class="competitor-name">{{ opp.competitor }}</div>
                        <div style="color: #6b7280; font-size: 14px; margin-top: 5px;">
                            Effective: {{ opp.effective_date }}
                        </div>
                    </div>
                    <div class="rate-badge">+{{ opp.their_increase }}%</div>
                </div>
                
                <div class="rate-comparison">
                    <div class="rate-item">
                        <div class="rate-label">Their Increase</div>
                        <div class="rate-value their-rate">+{{ opp.their_increase }}%</div>
                    </div>
                    <div class="rate-item">
                        <div class="rate-label">Your Rate</div>
                        <div class="rate-value your-rate">{{ your_rate_display }}</div>
                    </div>
                    <div class="rate-item">
                        <div class="rate-label">Your Advantage</div>
                        <div class="rate-value advantage">{{ opp.advantage }}%</div>
                    </div>
                </div>
                
                {% if opp.impact_score %}
                <div class="impact-meter">
                    <div class="impact-label">IMPACT SCORE</div>
                    <div class="impact-bar">
                        <div class="impact-fill" style="width: {{ opp.impact_score }}%"></div>
                    </div>
                    <div class="impact-details">
                        <span>{{ opp.impact_score }}/100</span>
                        <span>{{ opp.policies_affected|default('Est. policies affected') }}</span>
                    </div>
                </div>
                {% endif %}
                
                <div style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #e5e7eb;">
                    <strong>Key Details:</strong>
                    <ul style="margin-top: 10px; margin-left: 20px; color: #4b5563;">
                        <li>Product Line: {{ opp.product_line|default('Auto') }}</li>
                        <li>Filing Type: {{ opp.filing_type|default('Rate Revision') }}</li>
                        {% if opp.last_increase_date %}
                        <li>Their Last Increase: {{ opp.last_increase_date }} ({{ opp.last_increase }}%)</li>
                        {% endif %}
                    </ul>
                </div>
            </div>
            {% endfor %}
        </div>
        
        <div class="timeline">
            <h2>Action Timeline</h2>
            <p style="color: #6b7280; margin-bottom: 20px;">Optimal windows for customer outreach based on effective dates</p>
            
            {% for item in timeline %}
            <div class="timeline-item">
                <div class="timeline-date">{{ item.date }}</div>
                <div class="timeline-content">
                    <strong>{{ item.carrier }}</strong> rate increase takes effect
                    <span class="timeline-badge">{{ item.days_until }} days</span>
                    <div style="color: #6b7280; font-size: 14px; margin-top: 5px;">
                        Begin outreach 30-60 days prior for maximum impact
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
        
        <div style="margin-top: 40px; padding: 20px; background: #f9fafb; border-radius: 8px; text-align: center;">
            <p style="color: #6b7280; font-size: 14px;">
                Generated {{ timestamp }} | Data current as of {{ data_freshness }}<br>
                Next update: {{ next_update }}
            </p>
        </div>
    </div>
</body>
</html>
        """)
    
    def get_connection(self):
        return duckdb.connect(self.db_path)
    
    def generate_agent_report(self, agent_carrier, state, months_back=6):
        """Generate competitive intelligence report for an agent"""
        
        # Get agent's carrier rate
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
        
        # Get agent's recent rate change
        agent_rate_sql = f"""
        SELECT 
            AVG(Premium_Change_Number) * 100 as avg_rate,
            MAX(Effective_Date) as latest_date,
            COUNT(*) as filing_count
        FROM filings
        WHERE Company = '{exact_carrier}'
        AND State = '{state}'
        AND Effective_Date >= CURRENT_DATE - INTERVAL '{months_back} months'
        """
        agent_rate_result = conn.execute(agent_rate_sql).fetchone()
        agent_rate = agent_rate_result[0] if agent_rate_result[0] else 0
        
        # Get competitor opportunities
        opportunities_sql = f"""
        WITH recent_filings AS (
            SELECT 
                Company,
                ROUND(AVG(Premium_Change_Number) * 100, 1) as avg_increase,
                MAX(Premium_Change_Number) * 100 as max_increase,
                MAX(Effective_Date) as effective_date,
                COUNT(*) as filing_count,
                SUM(Policyholders_Affected_Number) as policies_affected,
                AVG(Impact_Score) as avg_impact_score,
                MAX(Previous_Increase_Date) as last_increase_date,
                MAX(Previous_Increase_Percentage) as last_increase,
                STRING_AGG(DISTINCT Product_Line, ', ') as product_lines
            FROM filings
            WHERE State = '{state}'
            AND Company != '{exact_carrier}'
            AND Premium_Change_Number > 0
            AND Effective_Date >= CURRENT_DATE - INTERVAL '{months_back} months'
            AND Effective_Date >= CURRENT_DATE  -- Future effective dates
            GROUP BY Company
        ),
        opportunities AS (
            SELECT 
                *,
                avg_increase - {agent_rate} as rate_advantage
            FROM recent_filings
            WHERE avg_increase > {agent_rate} + 2  -- At least 2% higher
        )
        SELECT * FROM opportunities
        ORDER BY rate_advantage DESC, avg_impact_score DESC
        LIMIT 10
        """
        
        opportunities_df = conn.execute(opportunities_sql).fetchdf()
        
        # Get market summary
        market_summary_sql = f"""
        SELECT 
            COUNT(DISTINCT Company) as total_carriers,
            AVG(CASE WHEN Premium_Change_Number > 0 THEN Premium_Change_Number * 100 END) as avg_increase,
            COUNT(*) as total_filings,
            SUM(Policyholders_Affected_Number) as total_affected
        FROM filings
        WHERE State = '{state}'
        AND Effective_Date >= CURRENT_DATE - INTERVAL '{months_back} months'
        """
        market_summary = conn.execute(market_summary_sql).fetchone()
        
        # Get agent's market position
        position_sql = f"""
        WITH carrier_rates AS (
            SELECT 
                Company,
                AVG(Premium_Change_Number) * 100 as avg_rate
            FROM filings
            WHERE State = '{state}'
            AND Effective_Date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY Company
            HAVING COUNT(*) >= 1
        ),
        ranked AS (
            SELECT 
                Company,
                avg_rate,
                ROW_NUMBER() OVER (ORDER BY avg_rate ASC) as position
            FROM carrier_rates
        )
        SELECT position
        FROM ranked
        WHERE Company = '{exact_carrier}'
        """
        position_result = conn.execute(position_sql).fetchone()
        agent_position = position_result[0] if position_result else 'N/A'
        
        conn.close()
        
      # Build template data
        opportunities = []
        urgent = []
        timeline = []

        for _, opp in opportunities_df.iterrows():
            opp_data = {
                'competitor': self._clean_company_name(opp['Company']),
                'their_increase': round(opp['avg_increase'], 1),
                'advantage': round(opp['rate_advantage'], 1),
                'effective_date': pd.to_datetime(opp['effective_date']).strftime('%B %d, %Y') if pd.notna(opp['effective_date']) else 'TBD',
                'impact_score': None,  # or int(opp['avg_impact_score']) if you have it
                'policies_affected': f"{int(opp['policies_affected']):,}" if pd.notna(opp['policies_affected']) else None,
                'product_line': opp['product_lines'],
                'last_increase_date': pd.to_datetime(opp['last_increase_date']).strftime('%B %Y') if pd.notna(opp['last_increase_date']) else None,
                'last_increase': f"+{opp['last_increase']}%" if pd.notna(opp['last_increase']) else None
            }
            opportunities.append(opp_data)

            # Check if urgent (effective within 60 days)
            if pd.notna(opp['effective_date']):
                days_until = (pd.to_datetime(opp['effective_date']) - pd.Timestamp.now()).days
                if 0 < days_until <= 60:
                    urgent.append(opp_data)

                # Add to timeline
                timeline.append({
                    'date': pd.to_datetime(opp['effective_date']).strftime('%b %d'),
                    'carrier': self._clean_company_name(opp['Company']),
                    'days_until': days_until
                })

        # Sort timeline by date
        timeline.sort(key=lambda x: x['days_until'])

        template_data = {
            'agent_carrier': agent_carrier,
            'state': state,
            'report_date': datetime.now().strftime('%B %d, %Y'),
            'total_filings': market_summary[2],
            'urgent_opportunities': urgent,
            'opportunities': opportunities[:5],  # Top 5
            'your_position': f"#{agent_position}",
            'total_carriers': market_summary[0],
            'winback_count': len(opportunities),
            'avg_competitor_increase': round(market_summary[1], 1) if market_summary[1] else 0,
            'your_rate_change': f"+{round(agent_rate, 1)}" if agent_rate > 0 else round(agent_rate, 1),
            'your_rate_display': f"+{round(agent_rate, 1)}%" if agent_rate > 0 else f"{round(agent_rate, 1)}%",
            'market_direction': "Hardening (Rates Rising)" if market_summary[1] > 5 else "Stable",
            'most_aggressive': opportunities[0]['competitor'] if opportunities else 'N/A',
            'total_policies_affected': f"{int(market_summary[3]):,}" if market_summary[3] else None,
            'timeline': timeline[:5],  # Next 5 effective dates
            'timestamp': datetime.now().strftime('%I:%M %p %Z'),
            'data_freshness': datetime.now().strftime('%B %d, %Y'),
            'next_update': (datetime.now() + timedelta(days=7)).strftime('%B %d, %Y')
        }
        return self.report_template.render(**template_data)
    
    def _clean_company_name(self, name):
        """Clean up company names for display"""
        # Fix spacing issues
        name = name.replace('InsuranceCompany', ' Insurance Company')
        name = name.replace('MutualAutomobile', ' Mutual Automobile')
        name = name.replace('andCasualty', ' and Casualty')
        name = name.replace('InsuranceCompany', ' Insurance Company')
        
        # Truncate if too long
        if len(name) > 40:
            name = name[:37] + '...'
        
        return name.strip()
    
    def save_report(self, html_content, filename=None):
        """Save report to file"""
        if filename is None:
            filename = f"agent_intel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        os.makedirs('reports', exist_ok=True)
        filepath = os.path.join('reports', filename)
        
        with open(filepath, 'w') as f:
            f.write(html_content)
        
        return filepath

# Generate sample reports
if __name__ == "__main__":
    generator = AgentIntelligenceReport()
    
    # Generate reports for different scenarios
    test_scenarios = [
        ("State Farm", "Arizona"),
        ("Allstate", "Illinois"),
        ("Progressive", "Texas")
    ]
    
    for carrier, state in test_scenarios:
        print(f"\nGenerating report for {carrier} agent in {state}...")
        
        html = generator.generate_agent_report(carrier, state)
        if html:
            filepath = generator.save_report(
                html, 
                f"agent_intel_{carrier.lower().replace(' ', '_')}_{state.lower()}_{datetime.now().strftime('%Y%m%d')}.html"
            )
            print(f"✅ Report saved: {filepath}")
            
            # Open the first one in browser
            if carrier == "State Farm":
                import webbrowser
                webbrowser.open(f"file://{os.path.abspath(filepath)}")
        else:
            print(f"❌ No data found for {carrier} in {state}")
