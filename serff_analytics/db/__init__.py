"""DuckDB utility helpers.

This module exposes :class:`DatabaseManager` for interacting with the local
analytics database.  A convenience :py:meth:`DatabaseManager.execute` helper is
provided to run a SQL statement using a short‑lived connection.
"""

import duckdb
import pandas as pd
from datetime import datetime
import logging
import os
from contextlib import contextmanager
from typing import Iterable, Any

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path="serff_analytics/data/insurance_filings.db"):
        self.db_path = db_path
        # Ensure the directory for the database exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_database()

    def get_connection(self):
        return duckdb.connect(self.db_path)

    @contextmanager
    def connection(self):
        """Context manager that yields a DuckDB connection and closes it."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self):
        """Context manager for a database transaction."""
        with self.connection() as conn:
            try:
                conn.begin()
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def execute(self, query: str, params: Iterable[Any] | None = None):
        """Run a SQL statement using a short-lived connection."""
        with self.connection() as conn:
            return conn.execute(query, params or [])

    def _drop_all_indexes(self, conn) -> None:
        """Remove all indexes on the filings table."""
        indexes = conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name='filings'"
        ).fetchall()
        for (idx,) in indexes:
            conn.execute(f'DROP INDEX IF EXISTS "{idx}"')

    def init_database(self):
        """Initialize database with proper schema"""
        try:
            with self.connection() as conn:
                info = conn.execute("PRAGMA table_info('filings')").fetchall()
                for row in info:
                    if row[1] == "Premium_Change_Number" and row[2].upper() == "DECIMAL(10,2)":
                        # Drop all dependent indexes before altering the column type
                        self._drop_all_indexes(conn)
                        conn.execute(
                            "ALTER TABLE filings ALTER COLUMN Premium_Change_Number SET DATA TYPE DECIMAL(10,4)"
                        )
                        logger.info("Migrated Premium_Change_Number to DECIMAL(10,4)")
                    if row[1] == "Previous_Increase_Percentage" and not row[2].upper().startswith(
                        "VARCHAR"
                    ):
                        self._drop_all_indexes(conn)
                        conn.execute(
                            "ALTER TABLE filings ALTER COLUMN Previous_Increase_Percentage SET DATA TYPE VARCHAR"
                        )
                        logger.info("Migrated Previous_Increase_Percentage to VARCHAR")
        except duckdb.CatalogException:
            # Table doesn't exist yet; it will be created below
            pass

        # Create main table matching your Airtable fields
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS filings (
                Record_ID VARCHAR PRIMARY KEY,
                Company VARCHAR,
                Subsidiary VARCHAR,
                State VARCHAR,
                Product_Line VARCHAR,
                Rate_Change_Type VARCHAR,
                Premium_Change_Number DECIMAL(10,4),
                Premium_Change_Amount_Text VARCHAR,
                Effective_Date DATE,
                Previous_Increase_Date DATE,
                Previous_Increase_Percentage VARCHAR,
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
            """
        )

        # Create indexes for performance
        self.execute("CREATE INDEX IF NOT EXISTS idx_state_product ON filings(State, Product_Line)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_company ON filings(Company, Subsidiary)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_effective_date ON filings(Effective_Date)")

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
