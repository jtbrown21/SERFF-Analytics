import os
from datetime import datetime
import logging
from jinja2 import Environment, FileSystemLoader
import duckdb

logger = logging.getLogger(__name__)


class StateNewsletterReport:
    """Generate state-specific newsletter reports"""

    def __init__(self, db_path="data/insurance_filings.db", template_dir="templates"):
        self.db_path = db_path
        self.env = Environment(loader=FileSystemLoader(template_dir))
        self.template = self.env.get_template("state_newsletter.html")

    def _get_connection(self):
        return duckdb.connect(self.db_path)

    def _market_summary(self, state: str):
        """Return summary metrics for a state"""
        query = f"""
            SELECT
                COUNT(DISTINCT Company) FILTER (WHERE Premium_Change_Number > 0) AS companies,
                ROUND(AVG(CASE WHEN Premium_Change_Number > 0 THEN Premium_Change_Number END) * 100, 1) AS avg_increase_pct,
                COALESCE(SUM(Policyholders_Affected_Number), 0) AS policies
            FROM filings
            WHERE State = ?
                AND Effective_Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                AND Effective_Date < DATE_TRUNC('month', CURRENT_DATE)
        """
        conn = self._get_connection()
        result = conn.execute(query, [state]).fetchone()
        conn.close()
        if result is None:
            return {"companies": 0, "avg_increase_pct": 0, "policies_affected": 0}
        companies, avg_pct, policies = result
        return {
            "companies": int(companies or 0),
            "avg_increase_pct": float(avg_pct or 0),
            "policies_affected": f"{int(policies or 0):,}",
        }

    def _rate_cards(self, state: str, limit: int = 3):
        """Return top rate changes for a state"""
        query = f"""
            SELECT
                Company,
                ROUND(MAX(Premium_Change_Number)*100,1) AS change_pct,
                COALESCE(SUM(Policyholders_Affected_Number),0) AS policies,
                MAX(Effective_Date) AS effective_date
            FROM filings
            WHERE State = ?
                AND Effective_Date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                AND Effective_Date < DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY Company
            ORDER BY change_pct DESC
            LIMIT {limit}
        """
        conn = self._get_connection()
        rows = conn.execute(query, [state]).fetchall()
        conn.close()
        cards = []
        for row in rows:
            company, pct, policies, eff = row
            color = "#ef4444" if pct >= 0 else "#10b981"
            cards.append(
                {
                    "company": company,
                    "change_pct": pct,
                    "policies": f"{int(policies):,}",
                    "effective_date": datetime.strptime(str(eff), "%Y-%m-%d").strftime("%b %d"),
                    "color": color,
                }
            )
        return cards

    def _overall_stats(self):
        query = """
            SELECT COUNT(*) AS total_filings,
                   COALESCE(SUM(Policyholders_Affected_Number),0) AS policyholders,
                   COUNT(DISTINCT State) AS states
            FROM filings
        """
        conn = self._get_connection()
        total_filings, policyholders, states = conn.execute(query).fetchone()
        conn.close()
        return {
            "total_filings": f"{int(total_filings):,}",
            "total_policyholders": f"{int(policyholders):,}",
            "states_active": int(states),
        }

    def generate(self, state: str):
        summary = self._market_summary(state)
        cards = self._rate_cards(state)
        stats = self._overall_stats()
        month = datetime.now().strftime("%B %Y")
        html = self.template.render(
            report_month=month,
            state=state,
            summary=summary,
            rate_cards=cards,
            stats=stats,
        )
        return html

    def save(self, html: str, filename: str):
        os.makedirs("reports", exist_ok=True)
        path = os.path.join("reports", filename)
        with open(path, "w") as f:
            f.write(html)
        logger.info("Report saved to %s", path)
        return path


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    state_arg = sys.argv[1] if len(sys.argv) > 1 else "Illinois"
    report = StateNewsletterReport()
    html = report.generate(state_arg)
    outfile = report.save(html, f"{state_arg.lower()}_report.html")
    print(f"Generated {outfile}")
