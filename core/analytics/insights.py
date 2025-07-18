import duckdb
import pandas as pd
from datetime import datetime, timedelta
import logging

from ..config.config import Config

logger = logging.getLogger(__name__)


class InsuranceAnalytics:
    def __init__(self, db_path=None):
        self.db_path = db_path or Config.DB_PATH

    def get_connection(self):
        return duckdb.connect(self.db_path)

    def market_overview(self, months_back=12):
        """Get comprehensive market overview"""
        conn = self.get_connection()

        # Overall statistics
        overview_sql = f"""
        WITH recent_filings AS (
            SELECT * FROM filings 
            WHERE Effective_Date >= CURRENT_DATE - INTERVAL '{months_back} months'
            AND Effective_Date <= CURRENT_DATE
        )
        SELECT 
            COUNT(*) as total_filings,
            COUNT(DISTINCT Company) as unique_companies,
            COUNT(DISTINCT State) as states_affected,
            ROUND(AVG(CASE WHEN Premium_Change_Number > 0 THEN Premium_Change_Number * 100 END), 2) as avg_increase_pct,
            ROUND(AVG(CASE WHEN Premium_Change_Number < 0 THEN Premium_Change_Number * 100 END), 2) as avg_decrease_pct,
            SUM(CASE WHEN Premium_Change_Number > 0 THEN 1 ELSE 0 END) as increases,
            SUM(CASE WHEN Premium_Change_Number < 0 THEN 1 ELSE 0 END) as decreases,
            SUM(CASE WHEN Premium_Change_Number = 0 THEN 1 ELSE 0 END) as no_change,
            ROUND(MAX(Premium_Change_Number) * 100, 2) as max_increase_pct,
            ROUND(MIN(Premium_Change_Number) * 100, 2) as max_decrease_pct
        FROM recent_filings
        """

        overview = conn.execute(overview_sql).fetchdf()
        conn.close()
        return overview

    def state_analysis(self, months_back=12):
        """Analyze rate changes by state"""
        conn = self.get_connection()

        state_sql = f"""
        WITH recent_filings AS (
            SELECT * FROM filings 
            WHERE Effective_Date >= CURRENT_DATE - INTERVAL '{months_back} months'
            AND Effective_Date <= CURRENT_DATE
        )
        SELECT 
            State,
            COUNT(*) as filing_count,
            ROUND(AVG(Premium_Change_Number * 100), 2) as avg_change_pct,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Premium_Change_Number * 100), 2) as median_change_pct,
            COUNT(DISTINCT Company) as companies_filing,
            SUM(CASE WHEN Premium_Change_Number > 0.1 THEN 1 ELSE 0 END) as large_increases,
            SUM(CASE WHEN Premium_Change_Number > 0 THEN 1 ELSE 0 END) as total_increases,
            SUM(CASE WHEN Premium_Change_Number < 0 THEN 1 ELSE 0 END) as total_decreases,
            ROUND(MAX(Premium_Change_Number) * 100, 2) as max_increase_pct,
            ROUND(MIN(Premium_Change_Number) * 100, 2) as max_decrease_pct
        FROM recent_filings
        GROUP BY State
        ORDER BY filing_count DESC
        """

        results = conn.execute(state_sql).fetchdf()
        conn.close()
        return results

    def company_rankings(self, months_back=12):
        """Rank companies by various metrics"""
        conn = self.get_connection()

        company_sql = f"""
        WITH recent_filings AS (
            SELECT * FROM filings 
            WHERE Effective_Date >= CURRENT_DATE - INTERVAL '{months_back} months'
            AND Effective_Date <= CURRENT_DATE
        ),
        company_stats AS (
            SELECT 
                Company,
                COUNT(*) as filing_count,
                COUNT(DISTINCT State) as states_active,
                ROUND(AVG(Premium_Change_Number * 100), 2) as avg_change_pct,
                ROUND(AVG(CASE WHEN Premium_Change_Number > 0 THEN Premium_Change_Number * 100 END), 2) as avg_increase_pct,
                SUM(CASE WHEN Premium_Change_Number > 0 THEN 1 ELSE 0 END) as increase_count,
                SUM(CASE WHEN Premium_Change_Number > 0.1 THEN 1 ELSE 0 END) as large_increase_count,
                SUM(Policyholders_Affected_Number) as total_policyholders_affected
            FROM recent_filings
            GROUP BY Company
        )
        SELECT *
        FROM company_stats
        WHERE filing_count >= 5  -- Only companies with meaningful activity
        ORDER BY avg_increase_pct DESC
        """

        results = conn.execute(company_sql).fetchdf()
        conn.close()
        return results

    def hot_zones_analysis(self):
        """Identify 'hot zones' - state/company combinations with aggressive rate increases"""
        conn = self.get_connection()

        hot_zones_sql = """
        WITH recent_filings AS (
            SELECT * FROM filings 
            WHERE Effective_Date >= CURRENT_DATE - INTERVAL '6 months'
            AND Premium_Change_Number > 0
        ),
        state_company_stats AS (
            SELECT 
                State,
                Company,
                COUNT(*) as filing_count,
                ROUND(AVG(Premium_Change_Number * 100), 2) as avg_increase_pct,
                ROUND(MAX(Premium_Change_Number * 100), 2) as max_increase_pct,
                SUM(Policyholders_Affected_Number) as policyholders_affected,
                STRING_AGG(DISTINCT Product_Line, ', ') as product_lines
            FROM recent_filings
            GROUP BY State, Company
            HAVING COUNT(*) >= 2  -- Multiple filings indicate pattern
            AND AVG(Premium_Change_Number) > 0.05  -- At least 5% average increase
        )
        SELECT *
        FROM state_company_stats
        ORDER BY avg_increase_pct DESC
        LIMIT 20
        """

        results = conn.execute(hot_zones_sql).fetchdf()
        conn.close()
        return results

    def trend_analysis(self):
        """Analyze trends over time"""
        conn = self.get_connection()

        trend_sql = """
        WITH monthly_stats AS (
            SELECT 
                DATE_TRUNC('month', Effective_Date) as month,
                COUNT(*) as filing_count,
                ROUND(AVG(Premium_Change_Number * 100), 2) as avg_change_pct,
                COUNT(DISTINCT Company) as unique_companies,
                COUNT(DISTINCT State) as states_affected,
                SUM(CASE WHEN Premium_Change_Number > 0 THEN 1 ELSE 0 END) as increases,
                SUM(CASE WHEN Premium_Change_Number < 0 THEN 1 ELSE 0 END) as decreases
            FROM filings
            WHERE Effective_Date >= CURRENT_DATE - INTERVAL '24 months'
            AND Effective_Date <= CURRENT_DATE
            GROUP BY DATE_TRUNC('month', Effective_Date)
        )
        SELECT 
            month,
            filing_count,
            avg_change_pct,
            unique_companies,
            states_affected,
            increases,
            decreases,
            ROUND(increases::FLOAT / NULLIF(filing_count, 0) * 100, 1) as increase_rate_pct
        FROM monthly_stats
        ORDER BY month
        """

        results = conn.execute(trend_sql).fetchdf()
        conn.close()
        return results

    def outlier_filings(self, threshold_pct=15):
        """Find extreme rate changes that might need attention"""
        conn = self.get_connection()

        outlier_sql = f"""
        SELECT 
            Company,
            State,
            Product_Line,
            ROUND(Premium_Change_Number * 100, 2) as change_pct,
            Effective_Date,
            Policyholders_Affected_Number,
            SERFF_Tracking_Number,
            CASE 
                WHEN Premium_Change_Number > {threshold_pct/100} THEN 'Large Increase'
                WHEN Premium_Change_Number < -{threshold_pct/100} THEN 'Large Decrease'
            END as outlier_type
        FROM filings
        WHERE ABS(Premium_Change_Number) > {threshold_pct/100}
        AND Effective_Date >= CURRENT_DATE - INTERVAL '6 months'
        ORDER BY ABS(Premium_Change_Number) DESC
        LIMIT 50
        """

        results = conn.execute(outlier_sql).fetchdf()
        conn.close()
        return results

    def competitive_positioning(self, company_name):
        """See how a company compares to market"""
        conn = self.get_connection()

        # First, find exact company name
        company_search = f"""
        SELECT DISTINCT Company 
        FROM filings 
        WHERE Company LIKE '%{company_name}%'
        LIMIT 5
        """
        companies = conn.execute(company_search).fetchdf()

        if companies.empty:
            conn.close()
            return pd.DataFrame()

        exact_company = companies.iloc[0]["Company"]

        positioning_sql = f"""
        WITH market_stats AS (
            SELECT 
                State,
                Product_Line,
                AVG(Premium_Change_Number * 100) as market_avg_pct,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Premium_Change_Number * 100) as market_median_pct,
                COUNT(DISTINCT Company) as competitor_count
            FROM filings
            WHERE Effective_Date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY State, Product_Line
        ),
        company_stats AS (
            SELECT 
                State,
                Product_Line,
                AVG(Premium_Change_Number * 100) as company_avg_pct,
                COUNT(*) as filing_count,
                MAX(Effective_Date) as latest_filing
            FROM filings
            WHERE Company = '{exact_company}'
            AND Effective_Date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY State, Product_Line
        )
        SELECT 
            c.State,
            c.Product_Line,
            c.company_avg_pct,
            m.market_avg_pct,
            ROUND(c.company_avg_pct - m.market_avg_pct, 2) as vs_market_pct,
            CASE 
                WHEN c.company_avg_pct > m.market_avg_pct + 2 THEN 'Above Market'
                WHEN c.company_avg_pct < m.market_avg_pct - 2 THEN 'Below Market'
                ELSE 'At Market'
            END as position,
            m.competitor_count,
            c.filing_count,
            c.latest_filing
        FROM company_stats c
        JOIN market_stats m ON c.State = m.State AND c.Product_Line = m.Product_Line
        ORDER BY c.filing_count DESC
        """

        results = conn.execute(positioning_sql).fetchdf()
        conn.close()
        return results


# Test the analytics
if __name__ == "__main__":
    analytics = InsuranceAnalytics()

    print("=== MARKET OVERVIEW ===")
    print(analytics.market_overview())

    print("\n=== TOP STATES BY ACTIVITY ===")
    print(analytics.state_analysis().head(10))

    print("\n=== COMPANY RANKINGS ===")
    print(analytics.company_rankings().head(10))

    print("\n=== HOT ZONES ===")
    print(analytics.hot_zones_analysis())
