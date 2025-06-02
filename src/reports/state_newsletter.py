import os
from datetime import datetime
import logging
from jinja2 import Environment, FileSystemLoader
import duckdb
from typing import Optional, Tuple
from src.database import get_month_boundaries

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

    @staticmethod
    def _parse_month(month_str: Optional[str]) -> Tuple[datetime, datetime, str]:
        """Return (start_date, end_date, label) for a YYYY-MM string or None."""
        if month_str:
            try:
                start = datetime.strptime(month_str, "%Y-%m")
            except ValueError:
                try:
                    start = datetime.strptime(month_str, "%Y-%m-%d")
                except ValueError:
                    start = datetime.now().replace(day=1)
        else:
            start = datetime.now().replace(day=1)

        # Compute first day of next month
        if start.month == 12:
            end = datetime(start.year + 1, 1, 1)
        else:
            end = datetime(start.year, start.month + 1, 1)

        label = start.strftime("%B %Y")
        return start, end, label

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

    def _get_product_line(self, state: str, start: datetime, end: datetime) -> str:
        """Return the most common product line for a state in the period."""
        query = """
            SELECT Product_Line, COUNT(*) as cnt
            FROM filings
            WHERE State = ?
              AND Effective_Date >= ?
              AND Effective_Date < ?
            GROUP BY Product_Line
            ORDER BY cnt DESC
            LIMIT 1
        """
        conn = self._get_connection()
        try:
            row = conn.execute(query, [state, start, end]).fetchone()
        except Exception as exc:
            logger.error("Product line query failed: %s", exc)
            conn.close()
            return ""
        conn.close()
        return row[0] if row else ""

    def _market_summary(self, state: str, start: datetime, end: datetime):
        """Return summary metrics for a state within a month"""
        query = f"""
            SELECT
                COUNT(DISTINCT Company) FILTER (WHERE Premium_Change_Number > 0) AS companies,
                ROUND(AVG(CASE WHEN Premium_Change_Number > 0 THEN Premium_Change_Number END) * 100, 1) AS avg_increase_pct,
                COALESCE(SUM(Policyholders_Affected_Number), 0) AS policies
            FROM filings
            WHERE State = ?
                AND Effective_Date >= ?
                AND Effective_Date < ?
        """
        conn = self._get_connection()
        try:
            result = conn.execute(query, [state, start, end]).fetchone()
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

    def _rate_cards(self, state: str, start: datetime, end: datetime, limit: int = 3):
        """Return top rate changes for a state"""
        query = f"""
            SELECT
                Company,
                Subsidiary,
                Impact_Score,
                ROUND(MAX(Premium_Change_Number)*100,1) AS change_pct,
                COALESCE(SUM(Policyholders_Affected_Number),0) AS policies,
                MAX(Effective_Date) AS effective_date
            FROM filings
            WHERE State = ?
                AND Effective_Date >= ?
                AND Effective_Date < ?
            GROUP BY Company, Subsidiary, Impact_Score
            ORDER BY Impact_Score DESC NULLS LAST, change_pct DESC
            LIMIT {limit}
        """
        conn = self._get_connection()
        rows = conn.execute(query, [state, start, end]).fetchall()
        conn.close()
        cards = []
        for row in rows:
            company, subsidiary, impact, pct, policies, eff = row
            color = "#ef4444" if (pct or 0) >= 0 else "#10b981"
            effective = "-"
            if eff:
                try:
                    effective = datetime.strptime(str(eff), "%Y-%m-%d").strftime("%b %d")
                except Exception:
                    effective = "-"
            display_name = (
                company if company and company != "Not specified in document" else subsidiary
            )
            cards.append(
                {
                    "company": display_name or "N/A",
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
            "carriers_tracked": (format_number_short(carriers) if carriers is not None else "0"),
        }

    def generate(self, state: str, month: Optional[str] = None):
        start, end, label = self._parse_month(month)
        start_bound, end_bound = get_month_boundaries(start.year, start.month)
        product_line = self._get_product_line(state, start, end)
        start_display = start_bound.strftime("%B %d, %Y")
        end_display = end_bound.strftime("%B %d, %Y")

        summary = self._market_summary(state, start, end) or {
            "companies": 0,
            "avg_increase_pct": 0,
            "policies_affected": "0",
        }
        cards = self._rate_cards(state, start, end) or []
        stats = self._overall_stats()
        if not stats:
            stats = {
                "total_filings": "0",
                "total_policyholders": "0",
                "carriers_tracked": "0",
            }
        month_label = label
        html = self.template.render(
            report_month=month_label,
            state=state,
            product_line=product_line,
            start_date=start_display,
            end_date=end_display,
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
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Generate a state newsletter")
    parser.add_argument("state", help="Target state")
    parser.add_argument("--month", dest="month", help="Target month YYYY-MM", required=False)
    args = parser.parse_args()

    report = StateNewsletterReport()
    html = report.generate(args.state, args.month)
    start, _, _ = report._parse_month(args.month)
    filename = f"{args.state.lower()}_{start.strftime('%B_%Y').lower()}.html"
    outfile = report.save(html, filename)
    print(f"Generated {outfile}")
