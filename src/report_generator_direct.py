import pandas as pd
from datetime import datetime, timedelta
from jinja2 import Template
import os
from src.analytics_direct import InsuranceAnalytics
import logging

logger = logging.getLogger(__name__)

class InsuranceReportGenerator:
    def __init__(self):
        self.analytics = InsuranceAnalytics()
        self.report_template = Template("""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body { 
                    font-family: Arial, sans-serif; 
                    margin: 20px;
                    background-color: #f5f5f5;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    padding: 30px;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                }
                h1 { 
                    color: #2c3e50; 
                    border-bottom: 3px solid #3498db;
                    padding-bottom: 10px;
                }
                h2 { 
                    color: #34495e; 
                    margin-top: 30px;
                    background: #ecf0f1;
                    padding: 10px;
                    border-left: 4px solid #3498db;
                }
                .metric-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin: 20px 0;
                }
                .metric-box {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px;
                    border-radius: 10px;
                    text-align: center;
                }
                .metric-value {
                    font-size: 36px;
                    font-weight: bold;
                    margin: 10px 0;
                }
                .metric-label {
                    font-size: 14px;
                    opacity: 0.9;
                }
                table {
                    border-collapse: collapse;
                    width: 100%;
                    margin: 20px 0;
                }
                th, td {
                    border: 1px solid #ddd;
                    padding: 12px;
                    text-align: left;
                }
                th {
                    background-color: #3498db;
                    color: white;
                    font-weight: bold;
                }
                tr:nth-child(even) { background-color: #f2f2f2; }
                tr:hover { background-color: #e8f4f8; }
                .insight-box {
                    background: #e8f4f8;
                    border-left: 4px solid #3498db;
                    padding: 15px;
                    margin: 20px 0;
                    border-radius: 5px;
                }
                .warning { 
                    background: #fee; 
                    border-left-color: #e74c3c;
                    color: #c0392b;
                }
                .success { 
                    background: #efe; 
                    border-left-color: #27ae60;
                    color: #27ae60;
                }
                .chart-container {
                    margin: 20px 0;
                    padding: 20px;
                    background: #f8f9fa;
                    border-radius: 10px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Insurance Rate Filing Analytics Report</h1>
                <p><strong>Report Period:</strong> {{ start_date }} to {{ end_date }}</p>
                <p><strong>Generated:</strong> {{ generation_time }}</p>
                
                <h2>Executive Summary</h2>
                <div class="metric-grid">
                    {{ metrics_html | safe }}
                </div>
                
                <div class="insight-box">
                    <h3>Key Findings</h3>
                    {{ key_findings | safe }}
                </div>
                
                <h2>State Market Analysis</h2>
                <p>Rate filing activity and trends by state over the past 12 months:</p>
                {{ state_table | safe }}
                
                <h2>Company Performance Rankings</h2>
                <p>Companies ranked by average rate increase (minimum 5 filings):</p>
                {{ company_table | safe }}
                
                <h2>Market Hot Zones</h2>
                <div class="insight-box warning">
                    <h3>Areas with Aggressive Rate Increases</h3>
                    <p>State/Company combinations showing significant rate increase patterns:</p>
                </div>
                {{ hot_zones_table | safe }}
                
                <h2>Outlier Analysis</h2>
                <p>Extreme rate changes requiring attention (>15% change):</p>
                {{ outliers_table | safe }}
                
                <h2>Market Trends</h2>
                <div class="chart-container">
                    <h3>Monthly Filing Trends</h3>
                    {{ trend_analysis | safe }}
                </div>
                
                <h2>Strategic Insights</h2>
                <div class="insight-box success">
                    {{ strategic_insights | safe }}
                </div>
                
                <hr>
                <p style="color: #7f8c8d; font-size: 12px; text-align: center;">
                    This report was automatically generated by the Insurance Analytics System.
                    Data accuracy depends on the completeness of rate filing submissions.
                </p>
            </div>
        </body>
        </html>
        """)
    
    def generate_comprehensive_report(self):
        """Generate a comprehensive HTML report"""
        logger.info("Generating comprehensive insurance analytics report...")
        
        # Get data
        overview = self.analytics.market_overview()
        states = self.analytics.state_analysis()
        companies = self.analytics.company_rankings()
        hot_zones = self.analytics.hot_zones_analysis()
        outliers = self.analytics.outlier_filings()
        trends = self.analytics.trend_analysis()
        
        # Generate metrics HTML
        metrics_html = self._generate_metrics_html(overview)
        
        # Generate key findings
        key_findings = self._generate_key_findings(overview, states, companies)
        
        # Generate strategic insights
        strategic_insights = self._generate_strategic_insights(states, companies, hot_zones)
        
        # Format tables
        state_table = self._format_state_table(states.head(15))
        company_table = self._format_company_table(companies.head(20))
        hot_zones_table = self._format_hot_zones_table(hot_zones)
        outliers_table = self._format_outliers_table(outliers.head(20))
        trend_analysis = self._format_trend_analysis(trends)
        
        # Render report
        html = self.report_template.render(
            start_date=(datetime.now() - timedelta(days=365)).strftime('%B %d, %Y'),
            end_date=datetime.now().strftime('%B %d, %Y'),
            generation_time=datetime.now().strftime('%B %d, %Y at %I:%M %p'),
            metrics_html=metrics_html,
            key_findings=key_findings,
            state_table=state_table,
            company_table=company_table,
            hot_zones_table=hot_zones_table,
            outliers_table=outliers_table,
            trend_analysis=trend_analysis,
            strategic_insights=strategic_insights
        )
        
        return html
    
    def _generate_metrics_html(self, overview):
        """Generate metric boxes HTML"""
        if overview.empty:
            return ""
        
        row = overview.iloc[0]
        metrics = [
            ("Total Filings", f"{int(row['total_filings']):,}"),
            ("Avg Increase", f"{row['avg_increase_pct']:.1f}%"),
            ("Companies", f"{int(row['unique_companies'])}"),
            ("States", f"{int(row['states_affected'])}"),
            ("Rate Increases", f"{int(row['increases']):,}"),
            ("Rate Decreases", f"{int(row['decreases']):,}")
        ]
        
        html = ""
        for label, value in metrics:
            html += f"""
            <div class="metric-box">
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
            </div>
            """
        return html
    
    def _generate_key_findings(self, overview, states, companies):
        """Generate key findings from data"""
        findings = []
        
        if not overview.empty:
            row = overview.iloc[0]
            findings.append(f"📊 Market shows {row['avg_increase_pct']:.1f}% average rate increase across {int(row['unique_companies'])} companies")
            findings.append(f"📈 {int(row['increases'])} rate increases vs {int(row['decreases'])} decreases ({row['increases']/row['total_filings']*100:.1f}% increase rate)")
        
        if not states.empty:
            top_state = states.iloc[0]
            findings.append(f"🌟 {top_state['State']} leads with {int(top_state['filing_count'])} filings and {top_state['avg_change_pct']:.1f}% average change")
            
            aggressive_states = states[states['avg_change_pct'] > 10].head(3)
            if not aggressive_states.empty:
                state_list = ', '.join(aggressive_states['State'].tolist())
                findings.append(f"⚠️ Aggressive rate increases in: {state_list} (all >10% average)")
        
        if not companies.empty:
            top_company = companies.iloc[0]
            findings.append(f"🏢 {top_company['Company'].split()[0]}... leads with {top_company['avg_increase_pct']:.1f}% average increase across {int(top_company['states_active'])} states")
        
        return "<ul>" + "\n".join([f"<li>{f}</li>" for f in findings]) + "</ul>"
    
    def _generate_strategic_insights(self, states, companies, hot_zones):
        """Generate strategic business insights"""
        insights = []
        
        # Geographic insights
        if not states.empty:
            high_activity_states = states[states['filing_count'] > 50].head(5)
            if not high_activity_states.empty:
                insights.append(f"<h4>Geographic Opportunities</h4>")
                insights.append(f"<p>High-activity states ({', '.join(high_activity_states['State'].tolist())}) show consistent filing patterns, suggesting mature markets with established rate adjustment cycles.</p>")
        
        # Competitive insights
        if not companies.empty:
            aggressive_companies = companies[companies['avg_increase_pct'] > 10].head(5)
            if not aggressive_companies.empty:
                insights.append(f"<h4>Competitive Landscape</h4>")
                insights.append(f"<p>{len(aggressive_companies)} companies are pursuing aggressive rate increases (>10% average), potentially creating market opportunities for competitive positioning.</p>")
        
        # Hot zone insights
        if not hot_zones.empty:
            insights.append(f"<h4>Risk Areas</h4>")
            insights.append(f"<p>{len(hot_zones)} state/company combinations show concentrated rate increase activity. These 'hot zones' may face regulatory scrutiny or customer retention challenges.</p>")
        
        return "\n".join(insights)
    
    def _format_state_table(self, df):
        """Format state analysis table"""
        if df.empty:
            return "<p>No state data available</p>"
        
        # Select and rename columns for display
        display_df = df[[
            'State', 'filing_count', 'avg_change_pct', 'median_change_pct',
            'companies_filing', 'large_increases', 'total_increases', 'total_decreases'
        ]].copy()
        
        display_df.columns = [
            'State', 'Total Filings', 'Avg Change %', 'Median Change %',
            'Active Companies', 'Large Increases (>10%)', 'Total Increases', 'Total Decreases'
        ]
        
        return display_df.to_html(index=False, classes='data-table', escape=False)
    
    def _format_company_table(self, df):
        """Format company rankings table"""
        if df.empty:
            return "<p>No company data available</p>"
        
        display_df = df[[
            'Company', 'filing_count', 'states_active', 'avg_increase_pct',
            'increase_count', 'large_increase_count'
        ]].copy()
        
        display_df.columns = [
            'Company', 'Filings', 'States', 'Avg Increase %',
            'Total Increases', 'Large Increases (>10%)'
        ]
        
        # Truncate long company names
        display_df['Company'] = display_df['Company'].str[:50] + '...'
        
        return display_df.to_html(index=False, classes='data-table', escape=False)
    
    def _format_hot_zones_table(self, df):
        """Format hot zones table"""
        if df.empty:
            return "<p>No significant hot zones identified</p>"
        
        display_df = df[[
            'State', 'Company', 'avg_increase_pct', 'max_increase_pct',
            'filing_count', 'product_lines'
        ]].copy()
        
        display_df.columns = [
            'State', 'Company', 'Avg Increase %', 'Max Increase %',
            'Filings', 'Product Lines'
        ]
        
        # Truncate long company names
        display_df['Company'] = display_df['Company'].str[:40] + '...'
        
        return display_df.to_html(index=False, classes='data-table', escape=False)
    
    def _format_outliers_table(self, df):
        """Format outliers table"""
        if df.empty:
            return "<p>No significant outliers found</p>"
        
        display_df = df[[
            'Company', 'State', 'Product_Line', 'change_pct',
            'Effective_Date', 'Policyholders_Affected_Number'
        ]].copy()
        
        display_df.columns = [
            'Company', 'State', 'Product', 'Change %',
            'Effective Date', 'Policyholders Affected'
        ]
        
        # Format numbers
        display_df['Policyholders Affected'] = display_df['Policyholders Affected'].fillna(0).astype(int)
        display_df['Company'] = display_df['Company'].str[:40] + '...'
        
        return display_df.to_html(index=False, classes='data-table', escape=False)
    
    def _format_trend_analysis(self, df):
        """Format trend analysis"""
        if df.empty:
            return "<p>Insufficient data for trend analysis</p>"
        
        # Create a simple ASCII chart for trends
        html = "<pre style='font-family: monospace; background: #f5f5f5; padding: 10px;'>"
        html += "Monthly Rate Filing Trends\n"
        html += "=" * 50 + "\n\n"
        
        for _, row in df.iterrows():
            month = pd.to_datetime(row['month']).strftime('%Y-%m')
            bar_length = int(row['filing_count'] / df['filing_count'].max() * 30)
            bar = '█' * bar_length
            html += f"{month} | {bar} {int(row['filing_count'])} filings ({row['avg_change_pct']:.1f}% avg)\n"
        
        html += "</pre>"
        return html
    
    def save_report(self, html_content, filename=None):
        """Save report to file"""
        if filename is None:
            filename = f"insurance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        os.makedirs('reports', exist_ok=True)
        filepath = os.path.join('reports', filename)
        
        with open(filepath, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Report saved to {filepath}")
        return filepath

# Test the report generator
if __name__ == "__main__":
    generator = InsuranceReportGenerator()
    html = generator.generate_comprehensive_report()
    filepath = generator.save_report(html)
    print(f"Report generated and saved to: {filepath}")
    
    # Open in browser (optional)
    import webbrowser
    webbrowser.open(f"file://{os.path.abspath(filepath)}")
