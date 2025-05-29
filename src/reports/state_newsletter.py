import os
from datetime import datetime
import logging
from jinja2 import Environment, FileSystemLoader
import duckdb

logger = logging.getLogger(__name__)


def format_number_short(value):
    """Convert large numbers to a shortened human readable format."""
    if value is None:
        return "N/A"
    try:
        value = int(value)
    except (TypeError, ValueError):
        return "N/A"

    if value < 1000:
        return str(value)
    if value < 1_000_000:
        return f"{value/1000:.1f}".rstrip("0").rstrip(".") + "K"
    if value < 1_000_000_000:
        return f"{value/1_000_000:.1f}".rstrip("0").rstrip(".") + "M"
    return f"{value/1_000_000_000:.1f}".rstrip("0").rstrip(".") + "B"


class StateNewsletterReport:
    """Generate state-specific newsletter reports"""

    def __init__(self, db_path="data/insurance_filings.db", template_dir="templates"):
        self.db_path = db_path
        self.env = Environment(loader=FileSystemLoader(template_dir))
        self.template = self.env.get_template("state_newsletter.html")

    def _get_connection(self):
        return duckdb.connect(self.db_path)

    def _get_carrier_count(self, conn):
        """Return unique carrier count or None on failure."""
        try:
            result = conn.execute("SELECT COUNT(DISTINCT Company) FROM filings").fetchone()
            return int(result[0]) if result else None
        except Exception as exc:
            logger.error("Carrier count query failed: %s", exc)
            return None

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
        try:
            result = conn.execute(query, [state]).fetchone()
        except Exception as exc:
            logger.error("Market summary query failed: %s", exc)
            conn.close()
            return {"companies": 0, "avg_increase_pct": 0, "policies_affected": "0"}

        conn.close()
        if result is None:
            return {"companies": 0, "avg_increase_pct": 0, "policies_affected": "0"}

        companies, avg_pct, policies = result
        return {
            "companies": int(companies or 0),
            "avg_increase_pct": float(avg_pct or 0),
            "policies_affected": format_number_short(policies or 0),
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
            color = "#ef4444" if (pct or 0) >= 0 else "#10b981"
            effective = "-"
            if eff:
                try:
                    effective = datetime.strptime(str(eff), "%Y-%m-%d").strftime("%b %d")
                except Exception:
                    effective = "-"
            cards.append(
                {
                    "company": company or "N/A",
                    "change_pct": pct if pct is not None else 0,
                    "policies": format_number_short(policies or 0),
                    "effective_date": effective,
                    "color": color,
                }
            )
        return cards

    def _overall_stats(self):
        query = """
            SELECT COUNT(*) AS total_filings,
                   COALESCE(SUM(Policyholders_Affected_Number),0) AS policyholders
            FROM filings
        """
        conn = self._get_connection()
        try:
            total_filings, policyholders = conn.execute(query).fetchone()
            carriers = self._get_carrier_count(conn)
        except Exception as exc:
            logger.error("Overall stats query failed: %s", exc)
            conn.close()
            return {
                "total_filings": "0",
                "total_policyholders": "0",
                "carriers_tracked": "0",
            }
        conn.close()
        return {
            "total_filings": format_number_short(total_filings),
            "total_policyholders": format_number_short(policyholders),
            "carriers_tracked": format_number_short(carriers) if carriers is not None else "0",
        }

    def generate(self, state: str):
        summary = self._market_summary(state) or {
            "companies": 0,
            "avg_increase_pct": 0,
            "policies_affected": "0",
        }
        cards = self._rate_cards(state) or []
        stats = self._overall_stats()
        if not stats:
            stats = {"total_filings": "0", "total_policyholders": "0", "carriers_tracked": "0"}
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
