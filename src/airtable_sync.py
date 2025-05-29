import pandas as pd
from pyairtable import Table
import duckdb
from datetime import datetime
import logging
from src.config import Config
from src.database import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirtableSync:
    def __init__(self):
        self.table = Table(
            Config.AIRTABLE_API_KEY,
            Config.AIRTABLE_BASE_ID,
            Config.AIRTABLE_TABLE_NAME
        )
        self.db = DatabaseManager(Config.DB_PATH)
    
    def sync_data(self):
        """Sync data from Airtable to DuckDB"""
        try:
            logger.info("Starting Airtable sync...")
            
            # Fetch all records
            records = self.table.all()
            logger.info(f"Fetched {len(records)} records from Airtable")
            
            # Convert to DataFrame
            data = []
            for record in records:
                fields = record['fields']
                # Map YOUR ACTUAL Airtable fields to database columns
                row = {
                    'Record_ID': record['id'],
                    'Company': fields.get('Parent Company', ''),
                    'Subsidiary': fields.get('Filing Company', ''),
                    'State': fields.get('Impacted State', ''),
                    'Product_Line': fields.get('Product Line', ''),
                    'Rate_Change_Type': fields.get('Rate Change Type', ''),
                    'Premium_Change_Number': self._parse_number(
                        fields.get('Overall Rate Change Number')
                    ),
                    'Premium_Change_Amount_Text': fields.get(
                        'Overall Rate Change', ''
                    ),
                    'Effective_Date': self._parse_date(
                        fields.get('Effective Date') or
                        fields.get('Effective Date (New)') or
                        fields.get('Effective Date Requested (New)')
                    ),
                    'Previous_Increase_Date': self._parse_date(
                        fields.get('Previous Increase Date')
                    ),
                    'Previous_Increase_Percentage': self._parse_number(
                        fields.get('Previous Increase Percentage')
                    ),
                    'Policyholders_Affected_Number': self._parse_number(
                        fields.get('Policyholders Affected Number')
                    ),
                    'Policyholders_Affected_Text': fields.get(
                        'Policyholders Affected Text', ''
                    ),
                    'Total_Written_Premium_Number': self._parse_number(
                        fields.get('Total Written Premium Number')
                    ),
                    'Impact_Score': self._parse_number(
                        fields.get('Impact Score')
                    ),
                    
                    'Total_Written_Premium_Text': fields.get(
                        'Total Written Premium Text', ''
                    ),
                    'SERFF_Tracking_Number': fields.get(
                        'SERFF Tracking Number', ''
                    ),
                    'Specific_Coverages': fields.get('Specific Coverages', ''),
                    'Filing_Method': fields.get('Filing Method', ''),
                    'Current_Status': fields.get('Current Status', ''),
                    'Date_Submitted': self._parse_date(
                        fields.get('Date Submitted')
                    ),
                    'Disposition_Date': self._parse_date(
                        fields.get('Disposition Date')
                    ),
                    'Stated_Reasons': fields.get('Name', ''),
                    'Population': fields.get('Population', ''),
                    'Renewals_Date': self._parse_date(
                        fields.get('Renewals Date')
                    ),
                    'Updated_At': datetime.now()
                }
                data.append(row)
            
            df = pd.DataFrame(data)
            logger.info(f"DataFrame created with {len(df)} rows and columns: {list(df.columns)}")
            
            # Load to DuckDB
            conn = self.db.get_connection()
            
            # First, let's check what columns exist in the table
            table_info = conn.execute("PRAGMA table_info(filings)").fetchall()
            db_columns = [col[1] for col in table_info]
            logger.info(f"Database columns: {db_columns}")
            
            # Only keep DataFrame columns that exist in the database
            df_columns = [col for col in df.columns if col in db_columns]
            df_filtered = df[df_columns]
            logger.info(f"Filtered DataFrame to columns: {df_columns}")
            
            # Clear existing data (for initial testing)
            conn.execute("DELETE FROM filings")
            
            # Insert new data with explicit columns
            columns_str = ', '.join(df_columns)
            placeholders = ', '.join(['?' for _ in df_columns])
            insert_query = f"INSERT INTO filings ({columns_str}) VALUES ({placeholders})"
            
            # Insert row by row (slower but more reliable for debugging)
            inserted = 0
            for idx, row in df_filtered.iterrows():
                try:
                    values = [row[col] for col in df_columns]
                    conn.execute(insert_query, values)
                    inserted += 1
                    if inserted % 500 == 0:
                        logger.info(f"Inserted {inserted} records...")
                except Exception as e:
                    logger.error(f"Failed to insert row {idx}: {e}")
                    logger.error(f"Row data: {row.to_dict()}")
                    # Continue with other rows
            
            # Get count
            total_records = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
            
            conn.close()
            
            logger.info(f"Sync completed. Total records in database: {total_records}")
            return {
                'success': True,
                'records_processed': len(df),
                'total_records': total_records
            }
            
        except Exception as e:
            logger.error(f"Sync failed: {type(e).__name__}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e)
            }
    
    def _parse_number(self, value):
        """Safely parse numeric values"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            return float(value)
        except:
            return None
    
    def _parse_date(self, value):
        """Safely parse date values"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            return pd.to_datetime(value).date()
        except:
            return None
