import duckdb
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def check_state_filing_completeness(data, start_year=2020, end_year=2024):
    """Return missing state/year combinations.

    Parameters
    ----------
    data : DataFrame | list[dict]
        Filing data with ``state_code`` and ``year`` fields. ``filing_status``
        is optional and not used directly.
    start_year : int, default 2020
        Beginning of the year range to validate.
    end_year : int, default 2024
        End of the year range (inclusive).

    This function previously only looked for ``NaN`` values when determining
    missing filings which meant a state with **no rows at all** for a given
    year (e.g. WA in 2024) was incorrectly treated as complete.  The new
    implementation cross joins all expected states with the specified year range
    so completely absent records are flagged.
    """

    df = pd.DataFrame(data)
    if not {"state_code", "year"}.issubset(df.columns):
        raise ValueError("Data must include 'state_code' and 'year' columns")

    df = df.dropna(subset=["state_code", "year"]).copy()
    df["state_code"] = df["state_code"].str.upper().str.strip()
    df["year"] = df["year"].astype(int)

    all_states = set(SimpleDataHealthCheck().ALL_STATES)

    missing = []
    for yr in range(start_year, end_year + 1):
        states_present = set(df.loc[df["year"] == yr, "state_code"].unique())
        for state in sorted(all_states - states_present):
            missing.append({"state": state, "year": yr})
    return missing


class SimpleDataHealthCheck:
    """Simple health check for insurance filings data.
    Note: Florida is excluded from all checks (no FL data available).
    """

    def __init__(self, db_path="data/insurance_filings.db"):
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
            "DC",
        ]

        self.NAME_TO_ABBR = {
            "Alabama": "AL",
            "Alaska": "AK",
            "Arizona": "AZ",
            "Arkansas": "AR",
            "California": "CA",
            "Colorado": "CO",
            "Connecticut": "CT",
            "Delaware": "DE",
            "Georgia": "GA",
            "Hawaii": "HI",
            "Idaho": "ID",
            "Illinois": "IL",
            "Indiana": "IN",
            "Iowa": "IA",
            "Kansas": "KS",
            "Kentucky": "KY",
            "Louisiana": "LA",
            "Maine": "ME",
            "Maryland": "MD",
            "Massachusetts": "MA",
            "Michigan": "MI",
            "Minnesota": "MN",
            "Mississippi": "MS",
            "Missouri": "MO",
            "Montana": "MT",
            "Nebraska": "NE",
            "Nevada": "NV",
            "New Hampshire": "NH",
            "New Jersey": "NJ",
            "New Mexico": "NM",
            "New York": "NY",
            "North Carolina": "NC",
            "North Dakota": "ND",
            "Ohio": "OH",
            "Oklahoma": "OK",
            "Oregon": "OR",
            "Pennsylvania": "PA",
            "Rhode Island": "RI",
            "South Carolina": "SC",
            "South Dakota": "SD",
            "Tennessee": "TN",
            "Texas": "TX",
            "Utah": "UT",
            "Vermont": "VT",
            "Virginia": "VA",
            "Washington": "WA",
            "West Virginia": "WV",
            "Wisconsin": "WI",
            "Wyoming": "WY",
            "District of Columbia": "DC",
        }

        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """Create indexes used for duplicate detection."""
        with duckdb.connect(self.db_path) as conn:
            try:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dup ON filings(Company, Subsidiary, State, Product_Line, Rate_Change_Type, Effective_Date)"
                )
                logger.debug("Indexes ensured on database")
            except duckdb.CatalogException:
                logger.debug("Filings table not found when ensuring indexes")

    def check_state_filing_completeness(self, start_year: int, end_year: int) -> list[dict]:
        """Return missing state/year combinations for the given range."""
        logger.debug("Checking state filing completeness from %s to %s", start_year, end_year)
        query = """
        SELECT DISTINCT State, YEAR(Effective_Date) AS year
        FROM filings
        WHERE Effective_Date IS NOT NULL
            AND YEAR(Effective_Date) BETWEEN ? AND ?
        """
        with duckdb.connect(self.db_path) as conn:
            df = conn.execute(query, [start_year, end_year]).fetchdf()

        df["state_abbr"] = df["State"].map(self.NAME_TO_ABBR).fillna(df["State"])

        all_states = set(self.ALL_STATES)
        missing: list[dict] = []
        for yr in range(start_year, end_year + 1):
            states_present = set(df.loc[df["year"] == yr, "state_abbr"].unique())
            for state in sorted(all_states - states_present):
                missing.append({"state": state, "year": yr})
        return missing

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
        with duckdb.connect(self.db_path) as conn:
            duplicates = conn.execute(query, params).fetchdf()
        return duplicates

    def get_year_overview(self) -> pd.DataFrame:
        """Show a quick overview of data by year."""
        query = """
        SELECT
            YEAR(Effective_Date) as year,
            COUNT(*) as total_filings,
            COUNT(DISTINCT State) as states_with_data,
            MIN(Effective_Date) as first_filing,
            MAX(Effective_Date) as last_filing
        FROM filings
        WHERE Effective_Date IS NOT NULL
            AND YEAR(Effective_Date) BETWEEN 2020 AND 2025
        GROUP BY YEAR(Effective_Date)
        ORDER BY year DESC
        """
        with duckdb.connect(self.db_path) as conn:
            overview = conn.execute(query).fetchdf()
        print("\U0001f4ca DATA OVERVIEW BY YEAR")
        print("=" * 50)
        print(f"{'Year':<6} {'Filings':<12} {'States':<15}")
        print("-" * 50)
        for _, row in overview.iterrows():
            year = int(row["year"])
            filings = f"{row['total_filings']:,}"
            states = f"{row['states_with_data']}/{len(self.ALL_STATES)}"
            print(f"{year:<6} {filings:<12} {states:<15}")
            missing_count = len(self.ALL_STATES) - row["states_with_data"]
            if 0 < missing_count < 10:
                missing_data = self.check_state_filing_completeness(year, year)
                missing_states = [m["state"] for m in missing_data]
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
        """Run the health checks and print results."""
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
        missing = []
        if year:
            print(f"\n\U0001f5fa\ufe0f  STATE COVERAGE FOR {year}")
            missing_data = self.check_state_filing_completeness(year, year)
            missing = [m["state"] for m in missing_data]
            if missing:
                print(f"\u274c {len(missing)} states missing: {', '.join(missing)}")
            else:
                print("\u2705 All expected states have data")
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
        default="data/insurance_filings.db",
        help="Path to DuckDB database",
    )
    parser.add_argument("--year", type=int, help="Limit checks to a single year", required=False)
    parser.add_argument("--overview", action="store_true", help="Show year overview")

    args = parser.parse_args()

    checker = SimpleDataHealthCheck(args.db_path)

    if args.overview:
        checker.get_year_overview()
    checker.run_health_check(args.year)
