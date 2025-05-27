import duckdb
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path='data/insurance_filings.db'):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        return duckdb.connect(self.db_path)
    
    def init_database(self):
        """Initialize database with proper schema"""
        conn = self.get_connection()
        
        # Create main table matching your Airtable fields
        conn.execute("""
            CREATE TABLE IF NOT EXISTS filings (
                Record_ID VARCHAR PRIMARY KEY,
                Company VARCHAR,
                Subsidiary VARCHAR,
                State VARCHAR,
                Product_Line VARCHAR,
                Rate_Change_Type VARCHAR,
                Premium_Change_Number DECIMAL(10,2),
                Premium_Change_Amount_Text VARCHAR,
                Effective_Date DATE,
                Previous_Increase_Date DATE,
                Previous_Increase_Percentage DECIMAL(10,2),
                Policyholders_Affected_Number INTEGER,
                Policyholders_Affected_Text VARCHAR,
                Total_Written_Premium_Number DECIMAL(15,2),
                Total_Written_Premium_Text VARCHAR,
                SERFF_Tracking_Number VARCHAR,
                Specific_Coverages VARCHAR,
                Stated_Reasons VARCHAR,
                Population VARCHAR,
                Impact_Score DECIMAL(10,2),
                Renewals_Date DATE,
                Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Updated_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_state_product ON filings(State, Product_Line)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_company ON filings(Company, Subsidiary)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_effective_date ON filings(Effective_Date)")
        
        conn.close()
        logger.info("Database initialized successfully")
    
    def get_schema_context(self):
        """Return schema information for LLM context"""
        return """
        Table: filings
        Key Columns:
        - Record_ID: Unique identifier
        - Company: Insurance company name (from 'Filing Company')
        - Subsidiary: Parent company name (from 'Parent Company')
        - State: US state code (from 'Impacted State')
        - Product_Line: Type of insurance (Auto, Homeowners, etc.)
        - Rate_Change_Type: 'increase' or 'decrease'
        - Premium_Change_Number: Percentage change (from 'Overall Rate Change Number')
        - Effective_Date: When the rate change takes effect
        - Previous_Increase_Date: Date of last increase
        - Policyholders_Affected_Number: Number of affected policyholders
        - Total_Written_Premium_Number: Total premium amount
        - SERFF_Tracking_Number: Regulatory tracking number
        """