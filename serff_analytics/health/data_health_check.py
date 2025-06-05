import duckdb
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SimpleDataHealthCheck:
    """Validate health of insurance filings data stored in DuckDB.

    The ``filings`` table is expected to contain full state names in the
    ``State`` column and filing dates between 2020 and 2024.  Florida filings
    are not available and therefore excluded from completeness checks. The
    District of Columbia is also ignored when calculating coverage.
    """

    def __init__(self, db_path="serff_analytics/data/insurance_filings.db"):
        self.db_path = db_path
        # All US states that should have insurance filings
        # NOTE: Florida (FL) excluded - no FL data currently available
        self.ALL_STATES = [
            "AL",
            "AK",
            "AZ",
            "AR",
            "CA",
            "CO",
            "CT",
            "DE",
            "GA",
            "HI",
            "ID",
            "IL",
            "IN",
            "IA",
            "KS",
            "KY",
            "LA",
            "ME",
            "MD",
            "MA",
            "MI",
            "MN",
            "MS",
            "MO",
            "MT",
            "NE",
            "NV",
            "NH",
            "NJ",
            "NM",
            "NY",
            "NC",
            "ND",
            "OH",
            "OK",
            "OR",
            "PA",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VT",
            "VA",
            "WA",
            "WV",
            "WI",
            "WY",
        ]

        self.NAME_TO_ABBR = {
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

        self.STATE_NORMALIZATION = {
            **{name: abbr for name, abbr in self.NAME_TO_ABBR.items()},
            **{abbr: abbr for abbr in self.NAME_TO_ABBR.values()},
        }

        self._ensure_indexes()

    def normalize_state(self, value: str | None) -> str | None:
        """Return a normalized state abbreviation or ``None``.

        Parameters
        ----------
        value : str | None
            Raw state value from the database.

        Returns
        -------
        str | None
            Two-letter abbreviation or ``None`` if the value can't be mapped or
            represents an excluded state.
        """
        if not value or not isinstance(value, str):
            return None
        key = value.strip().upper()
        if not key:
            return None
        abbr = self.STATE_NORMALIZATION.get(key)
        if not abbr:
            logger.warning("Unmapped state value '%s'", value)
            return None
        if abbr in {"FL", "DC"}:
            return None
        return abbr

    def _safe_query(self, query: str, params: list | tuple | None = None) -> pd.DataFrame:
        """Execute a SQL query safely and return a DataFrame."""
        try:
            with duckdb.connect(self.db_path) as conn:
                return conn.execute(query, params or []).fetchdf()
        except duckdb.CatalogException as exc:
            logger.error("Catalog error executing query: %s", exc)
        except Exception as exc:  # pragma: no cover - connection errors
            logger.error("Database error executing query: %s", exc)
        return pd.DataFrame()

    def _ensure_indexes(self) -> None:
        """Create indexes used for completeness and duplicate checks."""
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_filings_state_year ON filings(State, YEAR(Effective_Date))"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_filings_company_state_line_date ON filings(Company, State, Product_Line, Effective_Date)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dup ON filings(Company, Subsidiary, State, Product_Line, Rate_Change_Type, Effective_Date)"
                )
                logger.debug("Indexes ensured on database")
        except duckdb.CatalogException:
            logger.debug("Filings table not found when ensuring indexes")
        except Exception as exc:
            logger.error("Error ensuring indexes: %s", exc)

    def check_state_filing_completeness(self, start_year: int, end_year: int) -> dict:
        """Return missing state/year information and diagnostics."""
        logger.debug("Checking state filing completeness from %s to %s", start_year, end_year)
        query = """
        SELECT State, Effective_Date
        FROM filings
        WHERE (Effective_Date IS NOT NULL AND YEAR(Effective_Date) BETWEEN ? AND ?)
              OR Effective_Date IS NULL
        """
        df = self._safe_query(query, [start_year, end_year])

        records_processed = len(df)
        null_dates_count = int(df["Effective_Date"].isna().sum())

        df["state_abbr"] = df["State"].apply(self.normalize_state)
        unmapped_states = sorted(df.loc[df["state_abbr"].isna(), "State"].dropna().unique())
        df = df.dropna(subset=["state_abbr", "Effective_Date"]).copy()
        df["year"] = df["Effective_Date"].dt.year

        all_states = set(self.ALL_STATES)
        missing: list[dict] = []
        for yr in range(start_year, end_year + 1):
            states_present = set(df.loc[df["year"] == yr, "state_abbr"].dropna().unique())
            for state in sorted(all_states - states_present):
                missing.append({"state": state, "year": yr})

        return {
            "missing": missing,
            "unmapped_states": unmapped_states,
            "null_dates_count": null_dates_count,
            "records_processed": records_processed,
        }

    def check_perfect_duplicates(self, year: int | None = None) -> pd.DataFrame:
        """Find records where all tracked fields match exactly."""
        logger.debug("Checking duplicates for year %s", year if year else "ALL")
        year_filter = "WHERE YEAR(Effective_Date) = ?" if year else ""
        params: list[int] = [year] if year else []
        query = f"""
        WITH duplicate_groups AS (
            SELECT
                Company,
                Subsidiary,
                State,
                Product_Line,
                Rate_Change_Type,
                Premium_Change_Number,
                Premium_Change_Amount_Text,
                Effective_Date,
                Previous_Increase_Date,
                Previous_Increase_Percentage,
                Policyholders_Affected_Number,
                Policyholders_Affected_Text,
                Total_Written_Premium_Number,
                Total_Written_Premium_Text,
                SERFF_Tracking_Number,
                Specific_Coverages,
                Stated_Reasons,
                Population,
                Impact_Score,
                Renewals_Date,
                COUNT(*) as duplicate_count,
                STRING_AGG(Record_ID, ', ') as record_ids
            FROM filings
            {year_filter}
            GROUP BY
                Company, Subsidiary, State, Product_Line, Rate_Change_Type,
                Premium_Change_Number, Premium_Change_Amount_Text, Effective_Date,
                Previous_Increase_Date, Previous_Increase_Percentage,
                Policyholders_Affected_Number, Policyholders_Affected_Text,
                Total_Written_Premium_Number, Total_Written_Premium_Text,
                SERFF_Tracking_Number, Specific_Coverages, Stated_Reasons,
                Population, Impact_Score, Renewals_Date
            HAVING COUNT(*) > 1
        )
        SELECT * FROM duplicate_groups
        ORDER BY duplicate_count DESC, Company, State
        """
        duplicates = self._safe_query(query, params)
        return duplicates

    def get_year_overview(self) -> pd.DataFrame:
        """Show a quick overview of data by year."""
        query = """
        SELECT
            YEAR(Effective_Date) as year,
            COUNT(*) as total_filings,
            COUNT(DISTINCT State) as states_with_data,
            COUNT(DISTINCT Company) as unique_companies,
            AVG(Premium_Change_Number) as avg_premium_change,
            SUM(CASE WHEN Premium_Change_Number IS NULL THEN 1 ELSE 0 END) as missing_premium_change,
            SUM(CASE WHEN Policyholders_Affected_Number IS NULL THEN 1 ELSE 0 END) as missing_policyholders,
            MIN(Effective_Date) as first_filing,
            MAX(Effective_Date) as last_filing
        FROM filings
        WHERE Effective_Date IS NOT NULL
            AND YEAR(Effective_Date) BETWEEN 2020 AND 2025
        GROUP BY YEAR(Effective_Date)
        ORDER BY year DESC
        """
        overview = self._safe_query(query)
        print("\U0001f4ca DATA OVERVIEW BY YEAR")
        print("=" * 80)
        header = (
            f"{'Year':<6} {'Filings':<12} {'States':<15} {'Companies':<12} "
            f"{'Avg Change':<12} {'Missing Prem':<13} {'Missing PH':<12}"
        )
        print(header)
        print("-" * 80)
        for _, row in overview.iterrows():
            year = int(row["year"])
            filings = f"{row['total_filings']:,}"
            states = f"{row['states_with_data']}/{len(self.ALL_STATES)}"
            companies = f"{row['unique_companies']:,}"
            avg_change = (
                f"{row['avg_premium_change']:.2%}"
                if row["avg_premium_change"] is not None
                else "n/a"
            )
            miss_prem = f"{int(row['missing_premium_change']):,}"
            miss_ph = f"{int(row['missing_policyholders']):,}"
            print(
                f"{year:<6} {filings:<12} {states:<15} {companies:<12} {avg_change:<12} {miss_prem:<13} {miss_ph:<12}"
            )
            missing_count = len(self.ALL_STATES) - row["states_with_data"]
            if 0 < missing_count < 10:
                completeness = self.check_state_filing_completeness(year, year)
                missing_states = [m["state"] for m in completeness["missing"]]
                print(f"       Missing: {', '.join(missing_states)}")
        bad_years_query = """
        SELECT 
            YEAR(Effective_Date) as year,
            COUNT(*) as count
        FROM filings
        WHERE Effective_Date IS NOT NULL
            AND (YEAR(Effective_Date) < 2020 OR YEAR(Effective_Date) > 2025)
        GROUP BY YEAR(Effective_Date)
        ORDER BY count DESC
        """
        with duckdb.connect(self.db_path) as conn:
            bad_years = conn.execute(bad_years_query).fetchdf()
        if len(bad_years) > 0:
            print("\n\u26a0\ufe0f  Warning: Found filings with incorrect years:")
            for _, row in bad_years.iterrows():
                print(f"   - Year {int(row['year'])}: {row['count']} filings")
        return overview

    def run_health_check(self, year: int | None = None) -> None:
        """Run the health checks and print results.

        Parameters
        ----------
        year : int | None
            If provided, limit checks to this year.
        """
        logger.info("Running health check for %s", year if year else "all years")
        print("\n\U0001f3e5 INSURANCE DATA HEALTH CHECK")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if year:
            print(f"Year: {year}")
        print("=" * 50)
        print("\n\U0001f4cb DUPLICATE CHECK")
        duplicates = self.check_perfect_duplicates(year)
        if len(duplicates) > 0:
            total_duplicate_records = duplicates["duplicate_count"].sum() - len(duplicates)
            print(f"\u274c Found {len(duplicates)} groups of perfect duplicates")
            print(f"   Total duplicate records: {total_duplicate_records}")
            print("\n   Top duplicate examples:")
            for _, row in duplicates.head(5).iterrows():
                print(
                    f"   \u2022 {row['Company']} - {row['State']} - {row['Effective_Date']} ({row['duplicate_count']} copies)"
                )
        else:
            period = f" in {year}" if year else ""
            print(f"\u2705 No perfect duplicates found{period}")
        missing: list[str] = []
        if year:
            print(f"\n\U0001f5fa\ufe0f  STATE COVERAGE FOR {year}")
            completeness = self.check_state_filing_completeness(year, year)
            missing = [m["state"] for m in completeness["missing"]]
            if missing:
                print(f"\u274c {len(missing)} states missing: {', '.join(missing)}")
            else:
                print("\u2705 All expected states have data")
            if completeness["unmapped_states"]:
                print("   Unmapped states:", ", ".join(completeness["unmapped_states"]))
            if completeness["null_dates_count"]:
                print(f"   Records with null dates: {completeness['null_dates_count']}")
        print("\n" + "=" * 50)
        if year:
            print(f"SUMMARY FOR {year}:")
        else:
            print("OVERALL SUMMARY:")
        if len(duplicates) > 0:
            print("• \u274c Duplicates need cleanup")
        else:
            print("• \u2705 No duplicates")
        if year and missing:
            print(f"• \u274c Missing data for {len(missing)} states")
        elif year:
            print("• \u2705 All states covered")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Run data health checks")
    parser.add_argument(
        "--db-path",
        dest="db_path",
        default="serff_analytics/data/insurance_filings.db",
        help="Path to DuckDB database",
    )
    parser.add_argument("--year", type=int, help="Limit checks to a single year", required=False)
    parser.add_argument("--overview", action="store_true", help="Show year overview")

    args = parser.parse_args()

    checker = SimpleDataHealthCheck(args.db_path)

    if args.overview:
        checker.get_year_overview()
    checker.run_health_check(args.year)
