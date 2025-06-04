"""State-specific newsletter generator.

Run via::

    python -m serff_analytics.reports.state_newsletter <State> [--month YYYY-MM] [--test]

The optional ``--test`` flag enables verbose logging of all SQL queries and referenced records.
"""

import os
import subprocess
from datetime import datetime
import logging
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import duckdb
from typing import Optional, Tuple

# get_month_boundaries lives in the db utilities module
from serff_analytics.db.utils import get_month_boundaries
from serff_analytics.config import Config

logger = logging.getLogger(__name__)

# Mapping of U.S. state names to their two-letter abbreviations
STATE_ABBREVIATIONS = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}


def normalize_state_abbr(value: str) -> str:
    """Return the two-letter abbreviation for a state name or abbreviation."""
    if not value:
        return value
    key = value.strip().upper()
    return STATE_ABBREVIATIONS.get(key, key[:2])


class NoDataError(Exception):
    """Raised when no filings exist for the requested report period."""


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

    def __init__(
        self,
        db_path: Optional[str] = None,
        template_dir: str = "templates",
        test_mode: bool = False,
    ):
        """Initialize the report generator.

        Parameters
        ----------
        db_path: Optional[str]
            Location of the DuckDB database. Falls back to `Config.DB_PATH` if not provided.
        template_dir: str
            Directory containing the newsletter template.
        test_mode: bool
            Enable verbose logging of database queries and accessed records.
        """
        self.db_path = db_path or Config.DB_PATH
        self.env = Environment(loader=FileSystemLoader(template_dir))
        self.template = self.env.get_template("state_newsletter.html")
        self.test_mode = test_mode
        self.test_stats = {"query_count": 0, "records": 0, "table_counts": {}}
        self.last_filing_ids = []
        if self.test_mode:
            logger.info("[TEST MODE] Verbose database logging enabled")

    def _extract_table_name(self, query: str) -> str:
        """Return the first table name found in the query."""
        import re

        match = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
        return match.group(1) if match else "unknown"

    def _execute_query(
        self,
        conn,
        query: str,
        params: Optional[list] = None,
        func_name: str = "",
    ):
        """Execute a query and optionally log details in test mode."""

        params = params or []
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        if self.test_mode:
            table = self._extract_table_name(query)
            count = len(rows)
            self.test_stats["query_count"] += 1
            self.test_stats["records"] += count
            self.test_stats["table_counts"][table] = (
                self.test_stats["table_counts"].get(table, 0) + count
            )
            ids = []
            col_names = [desc[0] for desc in cursor.description]
            id_idx = None
            for idx, name in enumerate(col_names):
                if name.lower() in {"id", "record_id"}:
                    id_idx = idx
                    break
            if id_idx is not None:
                ids = [row[id_idx] for row in rows]
            logger.info("[TEST MODE] Database Query Log\n================================")
            logger.info("Function: %s()", func_name)
            logger.info("Query: %s", " ".join(query.strip().split()))
            logger.info("Records accessed: %d records from '%s' table", count, table)
            logger.info("Record IDs: %s", ids)

        return rows

    def log_summary(self):
        """Log aggregated query statistics in test mode."""

        if not self.test_mode:
            return
        tables = ", ".join(f"{tbl} ({cnt})" for tbl, cnt in self.test_stats["table_counts"].items())
        logger.info(
            "Summary:\n- Total queries executed: %d\n- Total records accessed: %d\n- Tables accessed: %s",
            self.test_stats["query_count"],
            self.test_stats["records"],
            tables,
        )

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
        """Return a DuckDB connection.

        Raises
        ------
        FileNotFoundError
            If the configured database file does not exist.
        """
        if not os.path.exists(self.db_path):
            logger.error(
                "Database file %s not found. Run 'python scripts/sync_demo.py' to initialize it.",
                self.db_path,
            )
            raise FileNotFoundError(f"Database file {self.db_path} not found")
        try:
            return duckdb.connect(self.db_path)
        except Exception as exc:
            logger.error("Failed to connect to database %s: %s", self.db_path, exc)
            raise

    def _get_carrier_count(self, conn):
        """Return unique carrier count or None on failure."""
        try:
            rows = self._execute_query(
                conn,
                "SELECT COUNT(DISTINCT Company) FROM filings",
                func_name="_get_carrier_count",
            )
            result = rows[0] if rows else None
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
            rows = self._execute_query(
                conn,
                query,
                [state, start, end],
                func_name="_get_product_line",
            )
            row = rows[0] if rows else None
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
                ROUND(AVG(Premium_Change_Number) FILTER (WHERE Premium_Change_Number > 0) * 100, 1) AS avg_increase_pct,
                COALESCE(SUM(Policyholders_Affected_Number) FILTER (WHERE Premium_Change_Number > 0), 0) AS policies
            FROM filings
            WHERE State = ?
                AND Effective_Date >= ?
                AND Effective_Date <= ?
        """
        conn = self._get_connection()
        try:
            rows = self._execute_query(
                conn,
                query,
                [state, start, end],
                func_name="_market_summary",
            )
            result = rows[0] if rows else None
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

    def _rate_cards(
        self, state: str, start: datetime, end: datetime, limit: int = 3
    ) -> Tuple[list, list]:
        """Return top rate changes for a state and the related filing IDs."""
        query = f"""
            SELECT
                Record_ID,
                Company,
                Subsidiary,
                Impact_Score,
                ROUND(Premium_Change_Number * 100, 1) AS change_pct,
                COALESCE(Policyholders_Affected_Number, 0) AS policies,
                Effective_Date
            FROM filings
            WHERE State = ?
                AND Effective_Date >= ?
                AND Effective_Date < ?
            ORDER BY Impact_Score DESC NULLS LAST, change_pct DESC
            LIMIT {limit}
        """
        conn = self._get_connection()
        rows = self._execute_query(
            conn,
            query,
            [state, start, end],
            func_name="_rate_cards",
        )
        conn.close()
        cards = []
        filing_ids = []
        for row in rows:
            rec_id, company, subsidiary, impact, pct, policies, eff = row
            filing_ids.append(rec_id)
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
        return cards, filing_ids

    def _overall_stats(self):
        query = """
            SELECT COUNT(*) AS total_filings,
                   COALESCE(SUM(Policyholders_Affected_Number),0) AS policyholders
            FROM filings
        """
        conn = self._get_connection()
        try:
            rows = self._execute_query(
                conn,
                query,
                func_name="_overall_stats",
            )
            total_filings, policyholders = rows[0]
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
        self.last_filing_ids = []

        # Check if any filings exist for the requested state and period
        conn = self._get_connection()
        try:
            rows = self._execute_query(
                conn,
                """
                    SELECT COUNT(*) FROM filings
                    WHERE State = ? AND Effective_Date >= ? AND Effective_Date < ?
                """,
                [state, start, end],
                func_name="generate_data_check",
            )
            count = rows[0][0]
        except Exception as exc:
            logger.error("Data availability check failed: %s", exc)
            conn.close()
            raise
        conn.close()
        if count == 0:
            logger.error(
                "No filings found for state %s between %s and %s",
                state,
                start,
                end,
            )
            raise NoDataError(
                "No filings available for this period. Please run the sync to populate the database."
            )

        start_bound, end_bound = get_month_boundaries(start.year, start.month)
        product_line = self._get_product_line(state, start, end)
        start_display = start_bound.strftime("%B %d, %Y")
        end_display = end_bound.strftime("%B %d, %Y")

        summary = self._market_summary(state, start, end) or {
            "companies": 0,
            "avg_increase_pct": 0,
            "policies_affected": "0",
        }
        cards, filing_ids = self._rate_cards(state, start, end)
        self.last_filing_ids = filing_ids
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

    def save(self, html: str, filename: str, output_dir: str = "reports"):
        """Write the HTML to ``filename`` within ``output_dir``."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        file_path = output_path / filename
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("Report saved to %s", file_path)
        return str(file_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate a state newsletter")
    parser.add_argument("state", help="Target state")
    parser.add_argument("--month", dest="month", help="Target month YYYY-MM", required=False)
    parser.add_argument("--test", action="store_true", help="Enable test mode with verbose logging")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.test else logging.WARNING)

    report = StateNewsletterReport(test_mode=args.test)
    html = report.generate(args.state, args.month)
    start, _, _ = report._parse_month(args.month)
    state_abbr = normalize_state_abbr(args.state)
    filename = f"{state_abbr}_{start.strftime('%m')}_{start.strftime('%Y')}.html"

    if args.test:
        output_dir = "reports"
        outfile = report.save(html, filename, output_dir=output_dir)
        report.log_summary()
        print(f"Generated {outfile}")
    else:
        year = start.strftime("%Y")
        month_full = start.strftime("%B")
        output_dir = (
            Path("docs") / "newsletters" / "monthly" / "19.0" / state_abbr / year / month_full
        )
        outfile = report.save(html, filename, output_dir=str(output_dir))

        report_url = (
            f"https://{os.getenv('GITHUB_USERNAME','USERNAME')}.github.io/"
            f"{os.getenv('GITHUB_REPO_NAME','SERFF-Analytics')}/newsletters/"
            f"monthly/19.0/{state_abbr}/{year}/{month_full}/{filename}"
        )

        from src.report_manager import ReportManager

        manager = ReportManager()
        manager.log_report(
            state=args.state,
            month=start.strftime("%B"),
            year=year,
            report_url=report_url,
            filings=report.last_filing_ids,
        )

        subprocess.run(["git", "add", outfile], check=False)
        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                f"Add {args.state} {start.strftime('%B %Y')} newsletter",
            ],
            check=False,
        )
        subprocess.run(["git", "push"], check=False)

        report.log_summary()
        print(f"Generated {outfile} and pushed to GitHub")
