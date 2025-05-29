import duckdb
import pandas as pd
from datetime import datetime


class SimpleDataHealthCheck:
    """Simple health check for insurance filings data."""

    def __init__(self, db_path="data/insurance_filings.db"):
        self.db_path = db_path
        # All US states except Florida (no FL data available)
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

    def check_missing_states(self, year=None):
        """Return a list of states with no filings for the period."""
        conn = duckdb.connect(self.db_path)
        if year:
            query = f"""
            SELECT DISTINCT State
            FROM filings
            WHERE State IS NOT NULL AND YEAR(Effective_Date) = {year}
            ORDER BY State
            """
            period_desc = f"in {year}"
        else:
            query = """
            SELECT DISTINCT State
            FROM filings
            WHERE State IS NOT NULL AND Effective_Date >= '2020-01-01'
            ORDER BY State
            """
            period_desc = "since Jan 1, 2020"
        states_with_data = conn.execute(query).fetchdf()
        conn.close()
        states_in_db = set(states_with_data["State"].tolist())
        missing_states = sorted(set(self.ALL_STATES) - states_in_db)
        print("=== MISSING STATES CHECK ===")
        print(f"Period: {period_desc}")
        print(f"States with data: {len(states_in_db)}/{len(self.ALL_STATES)}")
        if missing_states:
            print(f"\n⚠️  WARNING: {len(missing_states)} states have NO filings {period_desc}:")
            for state in missing_states:
                print(f"   - {state}")
        else:
            print(f"✓ All states have filings {period_desc}!")
        return missing_states

    def check_perfect_duplicates(self, year=None):
        """Find records where all fields match exactly."""
        conn = duckdb.connect(self.db_path)
        year_filter = f"WHERE YEAR(Effective_Date) = {year}" if year else ""

        # Dynamically build list of all columns except the primary key
        table_info = conn.execute("PRAGMA table_info(filings)").fetchall()
        all_columns = [row[1] for row in table_info]
        group_columns = [c for c in all_columns if c != "Record_ID"]
        cols = ", ".join(group_columns)

        query = f"""
        WITH duplicate_groups AS (
            SELECT
                {cols},
                COUNT(*) as duplicate_count,
                STRING_AGG(Record_ID, ', ') as record_ids
            FROM filings
            {year_filter}
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        )
        SELECT * FROM duplicate_groups
        ORDER BY duplicate_count DESC, Company, State
        """
        duplicates = conn.execute(query).fetchdf()
        conn.close()
        print("\n=== PERFECT DUPLICATES CHECK ===")
        if year:
            print(f"Year: {year}")
        if len(duplicates) > 0:
            total_duplicate_records = duplicates["duplicate_count"].sum() - len(duplicates)
            print(f"⚠️  WARNING: Found {len(duplicates)} groups of perfect duplicates")
            print(f"   Total duplicate records that could be removed: {total_duplicate_records}")
            print("\nTop 10 duplicate groups:")
            print("-" * 80)
            for idx, row in duplicates.head(10).iterrows():
                print(f"\nDuplicate Group {idx + 1}:")
                print(f"  Company: {row['Company']}")
                print(f"  State: {row['State']}")
                print(f"  Product Line: {row['Product_Line']}")
                print(f"  Effective Date: {row['Effective_Date']}")
                print(f"  Premium Change: {row['Premium_Change_Number']}%")
                print(f"  Copies: {row['duplicate_count']}")
                print(f"  Record IDs: {row['record_ids']}")
        else:
            period = f" in {year}" if year else ""
            print(f"✓ No perfect duplicates found{period}!")
        return duplicates

    def run_health_check(self, year=None):
        """Run both health checks."""
        print("DATA HEALTH CHECK REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if year:
            print(f"Year: {year}")
        print("=" * 80)
        missing_states = self.check_missing_states(year)
        duplicates = self.check_perfect_duplicates(year)
        print("\n" + "=" * 80)
        print("SUMMARY:")
        if missing_states:
            print(f"❌ {len(missing_states)} states missing data")
        else:
            print("✅ All states have data")
        if len(duplicates) > 0:
            print(f"❌ {len(duplicates)} groups of perfect duplicates found")
        else:
            print("✅ No perfect duplicates")

    def get_year_overview(self):
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
        GROUP BY YEAR(Effective_Date)
        ORDER BY year DESC
        """
        overview = conn.execute(query).fetchdf()
        conn.close()
        print("=== YEARLY DATA OVERVIEW ===")
        for _, row in overview.iterrows():
            missing = len(self.ALL_STATES) - row["states_with_data"]
            print(
                f"{int(row['year'])}: {row['total_filings']:,} filings | "
                f"{row['states_with_data']}/{len(self.ALL_STATES)} states | "
                f"Missing: {missing} states"
            )
        return overview


if __name__ == "__main__":
    checker = SimpleDataHealthCheck("data/insurance_filings.db")
    checker.get_year_overview()
    print("\n")
    checker.run_health_check()
    # To check specific year use:
    # checker.run_health_check(year=2024)
