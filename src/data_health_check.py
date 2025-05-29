import duckdb
import pandas as pd
from datetime import datetime


class SimpleDataHealthCheck:
    """Simple health check for insurance filings data.
    Note: Florida is excluded from all checks (no FL data available).
    """

    def __init__(self, db_path="data/insurance_filings.db"):
        self.db_path = db_path
        # All US state abbreviations that should have insurance filings
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

        # Mapping of full state names to abbreviations for lookups
        self.STATE_NAME_TO_ABBR = {
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
            "Florida": "FL",
        }

    def check_missing_states_by_year(self, year: int):
        """Return a list of states with no filings for a specific year."""
        conn = duckdb.connect(self.db_path)
        query = f"""
        SELECT DISTINCT State
        FROM filings
        WHERE State IS NOT NULL
            AND YEAR(Effective_Date) = {year}
        ORDER BY State
        """
        states_with_data = conn.execute(query).fetchdf()
        conn.close()
        states_in_db = {
            self.STATE_NAME_TO_ABBR.get(state, state)
            for state in states_with_data["State"].tolist()
            if state
        }
        missing_states = sorted(set(self.ALL_STATES) - states_in_db)
        return missing_states

    def check_perfect_duplicates(self, year: int | None = None) -> pd.DataFrame:
        """Find records where all tracked fields match exactly."""
        conn = duckdb.connect(self.db_path)
        year_filter = f"WHERE YEAR(Effective_Date) = {year}" if year else ""
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
        duplicates = conn.execute(query).fetchdf()
        conn.close()
        return duplicates

    def get_year_overview(self) -> pd.DataFrame:
        """Show a quick overview of data by year."""
        conn = duckdb.connect(self.db_path)
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
                missing_states = self.check_missing_states_by_year(year)
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
        bad_years = conn.execute(bad_years_query).fetchdf()
        conn.close()
        if len(bad_years) > 0:
            print("\n\u26a0\ufe0f  Warning: Found filings with incorrect years:")
            for _, row in bad_years.iterrows():
                print(f"   - Year {int(row['year'])}: {row['count']} filings")
        return overview

    def run_health_check(self, year: int | None = None) -> None:
        """Run the health checks and print results."""
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
            missing = self.check_missing_states_by_year(year)
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
    checker = SimpleDataHealthCheck("data/insurance_filings.db")
    checker.get_year_overview()
    checker.run_health_check()
    print("\n" + "=" * 50 + "\n")
    checker.run_health_check(year=2024)
