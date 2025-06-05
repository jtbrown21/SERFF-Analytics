import pandas as pd
from pyairtable import Table
import duckdb
from datetime import datetime
import logging
from serff_analytics.config import Config
from serff_analytics.db import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirtableSync:
    def __init__(self):
        self.table = Table(
            Config.AIRTABLE_API_KEY,
            Config.AIRTABLE_BASE_ID,
            Config.AIRTABLE_TABLE_NAME,
        )
        self.db = DatabaseManager(Config.DB_PATH)

    def _fetch_records(self, since: datetime | None = None):
        """Stream records from Airtable page by page."""
        params: dict[str, str] = {}
        if since:
            params["filter_by_formula"] = f"LAST_MODIFIED_TIME() > '{since.isoformat()}'"

        for page in self.table.iterate(page_size=100, **params):
            yield page

    def sync_data(self, since: datetime | None = None):
        """Sync data from Airtable to DuckDB"""
        try:
            logger.info("Starting Airtable sync...")

            # Optional: Run diagnostics first
            # self.diagnose_rate_change_field()
            # self.debug_field_values("Overall Rate Change Number")

            # Fetch records, optionally filtered by last modified time
            pages = list(self._fetch_records(since))
            fetched_count = sum(len(p) for p in pages)
            logger.info(f"Fetched {fetched_count} records from Airtable")
            records = [r for page in pages for r in page]

            # Convert to DataFrame
            data = []
            parsing_errors = []

            for record in records:
                fields = record["fields"]
                # Map YOUR ACTUAL Airtable fields to database columns
                product_line = fields.get("Product Line", "")

                # Track parsing issues for Premium_Change_Number
                rate_change_raw = fields.get("Overall Rate Change Number")
                rate_change_parsed = self._parse_number(
                    rate_change_raw, "Overall Rate Change Number"
                )

                if rate_change_raw is not None and rate_change_parsed is None:
                    parsing_errors.append(
                        {
                            "record_id": record["id"],
                            "field": "Overall Rate Change Number",
                            "raw_value": rate_change_raw,
                            "value_type": type(rate_change_raw).__name__,
                        }
                    )

                row = {
                    "Record_ID": record["id"],
                    "Company": fields.get("Parent Company", ""),
                    "Subsidiary": fields.get("Filing Company", ""),
                    "State": fields.get("Impacted State", ""),
                    # Map Airtable "Product Line" directly to the database column
                    "Product_Line": product_line,
                    "Rate_Change_Type": fields.get("Rate Change Type", ""),
                    "Premium_Change_Number": rate_change_parsed,
                    "Premium_Change_Amount_Text": fields.get("Overall Rate Change", ""),
                    "Effective_Date": self._parse_date(
                        fields.get("Effective Date")
                        or fields.get("Effective Date (New)")
                        or fields.get("Effective Date Requested (New)")
                    ),
                    "Previous_Increase_Date": self._parse_date(
                        fields.get("Previous Increase Date")
                    ),
                    "Previous_Increase_Percentage": self._parse_number(
                        fields.get("Previous Increase Percentage"), "Previous Increase Percentage"
                    ),
                    "Policyholders_Affected_Number": self._parse_number(
                        fields.get("Policyholders Affected Number"), "Policyholders Affected Number"
                    ),
                    "Policyholders_Affected_Text": fields.get("Policyholders Affected Text", ""),
                    "Total_Written_Premium_Number": self._parse_number(
                        fields.get("Total Written Premium Number"), "Total Written Premium Number"
                    ),
                    "Impact_Score": self._parse_number(fields.get("Impact Score"), "Impact Score"),
                    "Total_Written_Premium_Text": fields.get("Total Written Premium Text", ""),
                    "SERFF_Tracking_Number": fields.get("SERFF Tracking Number", ""),
                    "Specific_Coverages": fields.get("Specific Coverages", ""),
                    "Filing_Method": fields.get("Filing Method", ""),
                    "Current_Status": fields.get("Current Status", ""),
                    "Date_Submitted": self._parse_date(fields.get("Date Submitted")),
                    "Disposition_Date": self._parse_date(fields.get("Disposition Date")),
                    "Stated_Reasons": fields.get("Name", ""),
                    "Population": fields.get("Population", ""),
                    "Renewals_Date": self._parse_date(fields.get("Renewals Date")),
                    "Updated_At": datetime.now(),
                }
                data.append(row)

            # Report parsing errors if any
            if parsing_errors:
                logger.warning(f"Found {len(parsing_errors)} records with parsing errors:")
                for error in parsing_errors[:5]:  # Show first 5
                    logger.warning(
                        f"  Record {error['record_id']}: {error['field']} = {repr(error['raw_value'])} ({error['value_type']})"
                    )
                if len(parsing_errors) > 5:
                    logger.warning(f"  ... and {len(parsing_errors) - 5} more")

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

            # Insert new data in bulk using a staging relation
            columns_str = ", ".join(df_columns)
            conn.register("staging", df_filtered[df_columns])
            try:
                insert_query = (
                    f"INSERT INTO filings ({columns_str}) " f"SELECT {columns_str} FROM staging"
                )
                conn.execute(insert_query)
                inserted = conn.execute("SELECT COUNT(*) FROM staging").fetchone()[0]
                failed_inserts = 0
                logger.info(f"Inserted {inserted} records...")
            finally:
                conn.unregister("staging")

            # Get count
            db_total_records = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]

            conn.close()

            logger.info(f"Sync completed. Total records in database: {db_total_records}")

            # Summary report
            result = {
                "success": True,
                "records_processed": len(df),
                "records_inserted": inserted,
                "total_records": db_total_records,
                "parsing_errors": len(parsing_errors),
                "failed_inserts": failed_inserts,
            }

            if parsing_errors or failed_inserts:
                result["warning"] = (
                    f"{len(parsing_errors)} parsing errors, {failed_inserts} insert failures"
                )

            return result

        except Exception as e:
            logger.error(f"Sync failed: {type(e).__name__}: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}

    def _parse_number(self, value, field_name=""):
        """Safely parse numeric values, including percentages stored as decimals"""
        if pd.isna(value) or value == "" or value is None:
            return None

        try:
            # If it's already a number (int or float), return it
            if isinstance(value, (int, float)):
                # For percentage fields stored as decimals (0.179 = 17.9%)
                # Airtable already provides them in the correct decimal format
                return float(value)

            # If it's a string, try to convert
            if isinstance(value, str):
                # Remove any whitespace
                value = value.strip()

                # Try direct conversion
                return float(value)

        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse number '{value}' from field '{field_name}': {e}")
            logger.debug(f"Value type: {type(value)}, Value repr: {repr(value)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing '{value}' from field '{field_name}': {e}")
            return None

    def _parse_date(self, value):
        """Safely parse date values"""
        if pd.isna(value) or value == "" or value is None:
            return None
        try:
            return pd.to_datetime(value).date()
        except:
            return None

    def diagnose_rate_change_field(self, sample_size=10):
        """Diagnose issues with the Overall Rate Change Number field"""
        logger.info("=== Diagnosing Overall Rate Change Number field ===")

        records = self.table.all()[:sample_size]

        issues = []
        valid_count = 0

        for i, record in enumerate(records):
            value = record["fields"].get("Overall Rate Change Number")
            parsed = self._parse_number(value, "Overall Rate Change Number")

            if parsed is not None:
                valid_count += 1
                logger.info(f"Record {i+1}: {value} -> {parsed} ✓")
            else:
                issues.append(
                    {
                        "record_id": record["id"],
                        "raw_value": value,
                        "value_type": type(value).__name__,
                    }
                )
                logger.warning(
                    f"Record {i+1}: {repr(value)} (type: {type(value).__name__}) -> None ✗"
                )

        logger.info(f"\nSummary: {valid_count}/{sample_size} records parsed successfully")

        if issues:
            logger.warning(f"Found {len(issues)} problematic records")
            for issue in issues[:5]:  # Show first 5 issues
                logger.warning(
                    f"  Record {issue['record_id']}: {repr(issue['raw_value'])} ({issue['value_type']})"
                )

        return valid_count, issues

    def debug_field_values(self, field_name="Overall Rate Change Number", limit=20):
        """Debug specific field values from Airtable"""
        records = self.table.all()[:limit]

        value_types = {}
        examples_by_type = {}

        for record in records:
            value = record["fields"].get(field_name)
            value_type = type(value).__name__
            value_repr = repr(value)

            # Count types
            value_types[value_type] = value_types.get(value_type, 0) + 1

            # Collect examples
            if value_type not in examples_by_type:
                examples_by_type[value_type] = []
            if len(examples_by_type[value_type]) < 3:  # Keep first 3 examples
                examples_by_type[value_type].append(value_repr)

        logger.info(f"\n=== Field '{field_name}' Analysis ===")
        logger.info(f"Value type distribution:")
        for vtype, count in value_types.items():
            logger.info(f"  {vtype}: {count} occurrences")
            if vtype in examples_by_type:
                logger.info(f"    Examples: {', '.join(examples_by_type[vtype])}")

    def _inspect_percentage_field(self, records_sample=5):
        """Inspect how Airtable returns percentage fields"""
        logger.info("Inspecting percentage field format from Airtable...")

        records = self.table.all()[:records_sample]  # Get first few records

        for i, record in enumerate(records):
            value = record["fields"].get("Overall Rate Change Number")
            logger.info(f"Record {i+1}:")
            logger.info(f"  Raw value: {repr(value)}")
            logger.info(f"  Type: {type(value)}")
            logger.info(f"  Parsed: {self._parse_number(value, 'Overall Rate Change Number')}")

    def validate_numeric_fields(self):
        """Validate all numeric fields in the dataset"""
        numeric_fields = [
            "Overall Rate Change Number",
            "Previous Increase Percentage",
            "Policyholders Affected Number",
            "Total Written Premium Number",
            "Impact Score",
        ]

        logger.info("=== Validating All Numeric Fields ===")
        records = self.table.all()

        field_stats = {}

        for field in numeric_fields:
            valid = 0
            invalid = 0
            invalid_examples = []

            for record in records:
                value = record["fields"].get(field)
                parsed = self._parse_number(value, field)

                if value is not None:  # Only count non-null values
                    if parsed is not None:
                        valid += 1
                    else:
                        invalid += 1
                        if len(invalid_examples) < 3:  # Keep first 3 examples
                            invalid_examples.append(
                                {
                                    "record_id": record["id"],
                                    "value": repr(value),
                                    "type": type(value).__name__,
                                }
                            )

            field_stats[field] = {
                "valid": valid,
                "invalid": invalid,
                "invalid_examples": invalid_examples,
            }

        # Report results
        logger.info("\nValidation Results:")
        for field, stats in field_stats.items():
            logger.info(f"\n{field}:")
            logger.info(f"  Valid: {stats['valid']}")
            logger.info(f"  Invalid: {stats['invalid']}")
            if stats["invalid_examples"]:
                logger.info("  Invalid examples:")
                for ex in stats["invalid_examples"]:
                    logger.info(f"    Record {ex['record_id']}: {ex['value']} ({ex['type']})")

        return field_stats


# Example usage:
if __name__ == "__main__":
    sync = AirtableSync()

    # Run diagnostics first
    sync.diagnose_rate_change_field()

    # Or debug field values
    sync.debug_field_values("Overall Rate Change Number")

    # Or validate all numeric fields
    sync.validate_numeric_fields()

    # Then run the sync
    result = sync.sync_data()
    print(f"Sync result: {result}")
