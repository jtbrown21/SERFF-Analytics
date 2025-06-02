# src/agent_report_v3.py
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import os
import subprocess
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np

class AgentIntelligenceReportV3:
    def __init__(self, db_path='data/insurance_filings.db'):
        self.db_path = db_path
        # MJML template for email-first design
        self.mjml_template = """
<mjml>
  <mj-head>
    <mj-font name="Inter" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;900&display=swap" />
    <mj-attributes>
      <mj-all font-family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif" />
      <mj-section padding="0" />
      <mj-text color="#1f2937" font-size="16px" line-height="1.6" />
    </mj-attributes>
    <mj-style>
      .hero-metric {
        font-size: 48px !important;
        font-weight: 900 !important;
        line-height: 1 !important;
        color: #111827 !important;
        margin: 0 !important;
      }
      .metric-label {
        font-size: 14px !important;
        font-weight: 600 !important;
        color: #6b7280 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        margin-top: 8px !important;
      }
      .card-shadow {
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06) !important;
        border-radius: 12px !important;
      }
      .accent-teal {
        color: #14b8a6 !important;
      }
      .bg-teal {
        background-color: #14b8a6 !important;
      }
      .explore-btn {
        background: linear-gradient(135deg, #14b8a6 0%, #0d9488 100%) !important;
        color: white !important;
        padding: 12px 24px !important;
        border-radius: 8px !important;
        text-decoration: none !important;
        display: inline-block !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
      }
      .explore-btn:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 12px rgba(20, 184, 166, 0.3) !important;
      }
      @media only screen and (max-width: 600px) {
        .hero-metric {
          font-size: 36px !important;
        }
      }
    </mj-style>
  </mj-head>
  
  <mj-body background-color="#f9fafb">
    <!-- Header -->
    <mj-section background-color="#ffffff" padding-top="24px" padding-bottom="0">
      <mj-column>
        <mj-text align="center" font-size="13px" color="#6b7280">
          {{ report_date }} • {{ agent_carrier }} • {{ state }}
        </mj-text>
      </mj-column>
    </mj-section>
    
    <!-- Hero Metrics Strip -->
    <mj-section background-color="#ffffff" padding="32px 16px">
      <mj-group>
        <mj-column width="33.33%" padding="0 16px">
          <mj-text align="center" css-class="card-shadow" padding="24px" background-color="#ffffff">
            <div class="hero-metric accent-teal">${{ revenue_opportunity }}</div>
            <div class="metric-label">Revenue Opportunity</div>
          </mj-text>
        </mj-column>
        <mj-column width="33.33%" padding="0 16px">
          <mj-text align="center" css-class="card-shadow" padding="24px" background-color="#ffffff">
            <div class="hero-metric">{{ win_back_count }}</div>
            <div class="metric-label">Win-Back Targets</div>
          </mj-text>
        </mj-column>
        <mj-column width="33.33%" padding="0 16px">
          <mj-text align="center" css-class="card-shadow" padding="24px" background-color="#ffffff">
            <div class="hero-metric">{{ urgent_actions }}</div>
            <div class="metric-label">Act This Week</div>
          </mj-text>
        </mj-column>
      </mj-group>
    </mj-section>
    
    <!-- Market Pulse with Animated Sparkline -->
    <mj-section background-color="#ffffff" padding="0 32px 32px">
      <mj-column>
        <mj-text font-size="24px" font-weight="700" padding-bottom="16px">
          Market Pulse
        </mj-text>
        <mj-image src="{{ sparkline_gif }}" alt="Market trend animation" width="600px" />
        <mj-text font-size="14px" color="#6b7280" padding-top="8px">
          Rate filing activity over the last 90 days • Your position: <span class="accent-teal" style="font-weight: 700;">#{{ market_position }}</span> of {{ total_carriers }}
        </mj-text>
      </mj-column>
    </mj-section>
    
    <!-- Opportunity Accordion -->
    <mj-section background-color="#f3f4f6" padding="32px">
      <mj-column>
        <mj-text font-size="24px" font-weight="700" padding-bottom="24px">
          Top Opportunities
        </mj-text>
        
        <!-- We'll use MJML accordions but they need to be structured differently -->
        <!-- For email compatibility, we'll create expandable sections with checkboxes -->
        {% for opp in opportunities %}
        <mj-wrapper padding="0 0 16px 0">
          <mj-section background-color="#ffffff" border-radius="12px" css-class="card-shadow">
            <mj-column>
              <mj-text padding="24px 24px 16px">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                  <div>
                    <div style="font-size: 20px; font-weight: 700; color: #111827;">{{ opp.carrier }}</div>
                    <div style="font-size: 14px; color: #6b7280; margin-top: 4px;">
                      Effective {{ opp.effective_date }} • {{ opp.days_until }} days
                    </div>
                  </div>
                  <div style="text-align: right;">
                    <div style="font-size: 32px; font-weight: 900; color: #dc2626;">+{{ opp.rate }}%</div>
                    <div style="font-size: 12px; color: #6b7280;">their increase</div>
                  </div>
                </div>
              </mj-text>
              
              <!-- Expandable details -->
              <mj-text padding="0 24px 24px">
                <details style="border-top: 1px solid #e5e7eb; padding-top: 16px; margin-top: 16px;">
                  <summary style="cursor: pointer; font-weight: 600; color: #14b8a6; list-style: none;">
                    View Details →
                  </summary>
                  <div style="margin-top: 16px;">
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                      <div>
                        <div style="font-size: 12px; color: #6b7280; text-transform: uppercase;">Your Advantage</div>
                        <div style="font-size: 24px; font-weight: 700; color: #059669;">{{ opp.advantage }}%</div>
                      </div>
                      <div>
                        <div style="font-size: 12px; color: #6b7280; text-transform: uppercase;">Commission Opp</div>
                        <div style="font-size: 24px; font-weight: 700; color: #14b8a6;">${{ opp.commission }}</div>
                      </div>
                    </div>
                    <div style="margin-top: 16px;">
                      <div style="font-size: 14px; font-weight: 600; margin-bottom: 8px;">Best Targets:</div>
                      <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                        {% for segment in opp.segments %}
                        <span style="background: #e0f2fe; color: #0369a1; padding: 4px 12px; border-radius: 16px; font-size: 13px;">
                          {{ segment }}
                        </span>
                        {% endfor %}
                      </div>
                    </div>
                  </div>
                </details>
              </mj-text>
            </mj-column>
          </mj-section>
        </mj-wrapper>
        {% endfor %}
      </mj-column>
    </mj-section>
    
    <!-- Action Timeline -->
    <mj-section background-color="#ffffff" padding="32px">
      <mj-column>
        <mj-text font-size="24px" font-weight="700" padding-bottom="24px">
          Action Timeline
        </mj-text>
        
        <mj-table>
          <tr style="border-bottom: 2px solid #e5e7eb;">
            <th style="padding: 12px; text-align: left; font-weight: 600; color: #6b7280; font-size: 12px; text-transform: uppercase;">When</th>
            <th style="padding: 12px; text-align: left; font-weight: 600; color: #6b7280; font-size: 12px; text-transform: uppercase;">Carrier</th>
            <th style="padding: 12px; text-align: left; font-weight: 600; color: #6b7280; font-size: 12px; text-transform: uppercase;">Action</th>
          </tr>
          {% for event in timeline %}
          <tr style="border-bottom: 1px solid #f3f4f6;">
            <td style="padding: 16px 12px; font-weight: 700; color: {% if event.urgent %}#dc2626{% else %}#111827{% endif %};">
              {{ event.date }}
            </td>
            <td style="padding: 16px 12px;">
              {{ event.carrier }}
            </td>
            <td style="padding: 16px 12px; font-size: 14px; color: #6b7280;">
              {{ event.action }}
            </td>
          </tr>
          {% endfor %}
        </mj-table>
      </mj-column>
    </mj-section>
    
    <!-- CTA Section -->
    <mj-section background-color="#f0fdf4" padding="48px 32px">
      <mj-column>
        <mj-text align="center" font-size="28px" font-weight="700" color="#111827" padding-bottom="16px">
          Ready to capture ${{ revenue_opportunity }}?
        </mj-text>
        <mj-text align="center" color="#6b7280" padding-bottom="24px">
          Access your personalized dashboard for real-time updates, detailed analytics, and downloadable call lists.
        </mj-text>
        <mj-button href="{{ dashboard_url }}" background-color="#14b8a6" color="white" font-size="16px" font-weight="600" padding="16px 32px" border-radius="8px">
          Explore Live Dashboard →
        </mj-button>
      </mj-column>
    </mj-section>
    
    <!-- Footer -->
    <mj-section padding="24px 32px">
      <mj-column>
        <mj-text align="center" font-size="12px" color="#9ca3af">
          Generated {{ timestamp }} • Data refreshed daily<br>
          <a href="{{ unsubscribe_url }}" style="color: #9ca3af;">Manage preferences</a> • 
          <a href="{{ help_url }}" style="color: #9ca3af;">Get help</a>
        </mj-text>
      </mj-column>
    </mj-section>
  </mj-body>
</mjml>
        """
    
    def generate_sparkline_gif(self, data, filename='sparkline.gif'):
        """Generate sparkline image (static for reliability)"""
        import matplotlib
        matplotlib.use('Agg')  # Use non-interactive backend
        import matplotlib.pyplot as plt

        # Create a beautiful static sparkline
        fig, ax = plt.subplots(figsize=(8, 2), facecolor='None')

        # Smooth the data for better visual
        x = range(len(data))

        # Create gradient effect
        ax.plot(x, data, '#14b8a6', linewidth=3, solid_capstyle='round')

        # Add gradient fill
        ax.fill_between(x, data, alpha=0.3, color='#14b8a6')

        # Add subtle grid
        ax.grid(True, alpha=0.1, linestyle='-', linewidth=0.5)

        # Style
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])

        # Add start and end dots
        ax.scatter([0, len(data)-1], [data[0], data[-1]], color='#14b8a6', s=50, zorder=5)

        # Save
        os.makedirs('reports/assets', exist_ok=True)
        filepath = f'reports/assets/{filename.replace(".gif", ".png")}'
        plt.savefig(filepath, transparent=True, bbox_inches='tight', pad_inches=0.1, dpi=200)
        plt.close()

        return filepath
    
    def get_connection(self):
        return duckdb.connect(self.db_path)
    
    def generate_agent_report(self, agent_carrier, state, dashboard_url="https://app.insureintell.com/dashboard"):
        """Generate MJML-based agent report"""
        
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
        
        # Get market trend data for sparkline
        trend_sql = """
        SELECT 
            DATE_TRUNC('week', Effective_Date) as week,
            COUNT(*) as filings
        FROM filings
        WHERE State = '{}'
        AND Effective_Date >= CURRENT_DATE - INTERVAL '90 days'
        AND Effective_Date <= CURRENT_DATE
        GROUP BY week
        ORDER BY week
        """.format(state)
        
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
        
        # Generate sparkline GIF
        sparkline_data = trend_df['filings'].tolist() if len(trend_df) > 0 else [0]
        sparkline_path = self.generate_sparkline_gif(sparkline_data)
        
        # Process opportunities
        opportunities = []
        timeline = []
        total_revenue = 0
        urgent_count = 0
        
        for _, opp in opportunities_df.iterrows():
            policies = int(opp['policies_affected']) if pd.notna(opp['policies_affected']) else 100
            commission = int(policies * 1200 * 0.15 * 0.65)  # Assume 65% win rate
            total_revenue += commission
            
            if opp['days_until'] <= 7:
                urgent_count += 1
            
            opp_data = {
                'carrier': self._clean_name(opp['Company']),
                'rate': round(opp['avg_increase'], 1),
                'advantage': round(opp['rate_advantage'], 1),
                'effective_date': pd.to_datetime(opp['effective_date']).strftime('%b %d'),
                'days_until': int(opp['days_until']),
                'commission': f"{commission:,}",
                'segments': ['Multi-car', 'Loyalty seekers', 'Price-sensitive'][:2]
            }
            opportunities.append(opp_data)
            
            # Timeline
            if opp['days_until'] <= 30:
                timeline.append({
                    'date': pd.to_datetime(opp['effective_date']).strftime('%b %d'),
                    'carrier': self._clean_name(opp['Company']),
                    'action': 'Begin outreach campaign',
                    'urgent': opp['days_until'] <= 7
                })
        
        # Build MJML data
        mjml_data = {
            'report_date': datetime.now().strftime('%B %d, %Y'),
            'agent_carrier': agent_carrier,
            'state': state,
            'revenue_opportunity': f"{total_revenue:,}",
            'win_back_count': len(opportunities_df),
            'urgent_actions': urgent_count,
            'sparkline_gif': f"cid:sparkline",  # Email attachment reference
            'market_position': market_position,
            'total_carriers': total_carriers,
            'opportunities': opportunities[:4],
            'timeline': timeline[:5],
            'dashboard_url': f"{dashboard_url}?agent={agent_carrier.lower()}&state={state.lower()}",
            'timestamp': datetime.now().strftime('%I:%M %p'),
            'unsubscribe_url': '#',
            'help_url': '#'
        }
        
        # Render MJML
        mjml_content = self.mjml_template
        for key, value in mjml_data.items():
            if isinstance(value, list):
                # Handle list templating manually
                if key == 'opportunities':
                    opp_html = ""
                    for opp in value:
                        # Build opportunity HTML
                        segments_html = ""
                        for seg in opp.get('segments', []):
                            segments_html += f'<span style="background: #e0f2fe; color: #0369a1; padding: 4px 12px; border-radius: 16px; font-size: 13px; margin-right: 8px;">{seg}</span>'
                        
                        opp['segments_html'] = segments_html
                    mjml_content = mjml_content.replace('{{ ' + key + ' }}', str(value))
                elif key == 'timeline':
                    mjml_content = mjml_content.replace('{{ ' + key + ' }}', str(value))
            else:
                mjml_content = mjml_content.replace('{{ ' + key + ' }}', str(value))
        
        # For now, return the MJML (would need to compile to HTML with mjml command)
        return mjml_content, sparkline_path
    
    def _clean_name(self, name):
        """Clean company names"""
        name = name.replace('InsuranceCompany', ' Insurance')
        name = name.replace('MutualAutomobile', ' Mutual')
        if len(name) > 25:
            name = name[:22] + '...'
        return name.strip()
    
    def compile_mjml(self, mjml_content, output_file):
        """Compile MJML to HTML using mjml CLI"""
        # Save MJML temporarily
        temp_mjml = 'reports/temp.mjml'
        with open(temp_mjml, 'w') as f:
            f.write(mjml_content)
        
        # Compile with mjml
        try:
            subprocess.run(['mjml', temp_mjml, '-o', output_file], check=True)
            os.remove(temp_mjml)
            return True
        except:
            print("MJML CLI not found. Install with: npm install -g mjml")
            # Fallback: save as MJML
            with open(output_file.replace('.html', '.mjml'), 'w') as f:
                f.write(mjml_content)
            return False
    
    def save_report(self, mjml_content, sparkline_path, filename=None):
        """Save and compile report"""
        if filename is None:
            filename = f"agent_intel_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        os.makedirs('reports', exist_ok=True)
        filepath = os.path.join('reports', filename)
        
        # Try to compile MJML
        if self.compile_mjml(mjml_content, filepath):
            print(f"✅ Compiled HTML saved: {filepath}")
        else:
            # Save raw MJML
            mjml_path = filepath.replace('.html', '.mjml')
            with open(mjml_path, 'w') as f:
                f.write(mjml_content)
            print(f"📄 MJML saved: {mjml_path}")
            print("   Install MJML to compile: npm install -g mjml")
            print(f"   Then run: mjml {mjml_path} -o {filepath}")
        
        return filepath

# Generate V3 reports
if __name__ == "__main__":
    generator = AgentIntelligenceReportV3()
    
    # Test scenario
    carrier = "State Farm"
    state = "Arizona"
    
    print(f"\nGenerating V3 report for {carrier} agent in {state}...")
    
    mjml_content, sparkline_path = generator.generate_agent_report(carrier, state)
    if mjml_content:
        filepath = generator.save_report(
            mjml_content, 
            sparkline_path,
            f"agent_intel_v3_{carrier.lower().replace(' ', '_')}_{state.lower()}_{datetime.now().strftime('%Y%m%d')}.html"
        )
        print(f"📊 Sparkline GIF: {sparkline_path}")
    else:
        print(f"❌ No data found for {carrier} in {state}")
