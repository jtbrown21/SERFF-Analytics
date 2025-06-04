import duckdb
import pandas as pd

# Connect to database
conn = duckdb.connect('serff_analytics/data/insurance_filings.db')

print("=== Data Quality Check ===\n")

# 1. Check record count
count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
print(f"Total records: {count}")

# 2. Check states
print("\nStates with most filings:")
states = conn.execute("""
    SELECT State, COUNT(*) as count 
    FROM filings 
    GROUP BY State 
    ORDER BY count DESC 
    LIMIT 10
""").fetchdf()
print(states)

# 3. Check rate changes
print("\nRate change statistics:")
rate_stats = conn.execute("""
    SELECT 
        COUNT(*) as total,
        AVG(Premium_Change_Number) as avg_change,
        MIN(Premium_Change_Number) as min_change,
        MAX(Premium_Change_Number) as max_change
    FROM filings
    WHERE Premium_Change_Number IS NOT NULL
""").fetchdf()
print(rate_stats)

# 4. Check companies
print("\nTop 10 companies by filing count:")
companies = conn.execute("""
    SELECT Company, COUNT(*) as count 
    FROM filings 
    GROUP BY Company 
    ORDER BY count DESC 
    LIMIT 10
""").fetchdf()
print(companies)

# 5. Check date ranges
print("\nDate range of filings:")
dates = conn.execute("""
    SELECT 
        MIN(Effective_Date) as earliest_date,
        MAX(Effective_Date) as latest_date
    FROM filings
    WHERE Effective_Date IS NOT NULL
""").fetchdf()
print(dates)

conn.close()
