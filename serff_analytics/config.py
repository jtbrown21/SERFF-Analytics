import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
    # Allow the database path to be overridden via environment variable
    DB_PATH = os.getenv("DATABASE_PATH", "data/insurance_filings.db")
