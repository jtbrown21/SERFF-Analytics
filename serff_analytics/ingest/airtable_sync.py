import pandas as pd
from pyairtable import Table
from datetime import datetime
import logging
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    before_sleep_log,
    RetryError,
)
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

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _safe_call(self, func, *args, **kwargs):
        """Call Airtable API methods with retries."""
        return func(*args, **kwargs)

    def _fetch_records(self, since: datetime | None = None):
        """Stream records from Airtable page by page."""
        params: dict[str, str] = {}
        if since:
            params["filter_by_formula"] = f"LAST_MODIFIED_TIME() > '{since.isoformat()}'"

        try:
            for page in self._safe_call(self.table.iterate, page_size=100, **params):
                yield page
        except RetryError as exc:
            logger.error(f"Failed to fetch records from Airtable: {exc}")
            raise

    def sync_data(self, since: datetime | None = None):
        """Sync data from Airtable to DuckDB"""
        try:
            logger.info("Starting Airtable sync...")

            total_processed = 0
            total_errors = 0
            all_dataframes: list[pd.DataFrame] = []

            with self.db.connection() as conn:
                table_info = conn.execute("PRAGMA table_info(filings)").fetchall()
                db_columns = [col[1] for col in table_info]

            for page_num, page in enumerate(self._fetch_records(since), start=1):
                df, errors = self._process_page(page, page_num, db_columns)
                if not df.empty:
                    all_dataframes.append(df)
                total_processed += len(page)
                total_errors += errors

            combined_df = (
                pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()
            )

            with self.db.transaction() as conn:
                inserted = 0
                if not combined_df.empty:
                    columns_str = ", ".join(combined_df.columns)
                    placeholders = ", ".join(["?"] * len(combined_df))
                    conn.execute(
                        f"DELETE FROM filings WHERE Record_ID IN ({placeholders})",
                        combined_df["Record_ID"].tolist(),
                    )
                    conn.register("staging", combined_df)
                    try:
                        conn.execute(
                            f"INSERT INTO filings ({columns_str}) SELECT {columns_str} FROM staging"
                        )
                        inserted = len(combined_df)
                    finally:
                        conn.unregister("staging")
                db_total_records = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]

            logger.info(f"Sync completed. Total records in database: {db_total_records}")

            result = {
                "success": True,
                "records_processed": total_processed,
                "records_inserted": inserted,
                "total_records": db_total_records,
                "parsing_errors": total_errors,
                "failed_inserts": 0,
            }

            if total_errors:
                result["warning"] = f"{total_errors} parsing errors"

            return result

        except Exception as e:
            logger.error(f"Sync failed: {type(e).__name__}: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}

    def _process_page(self, page: list[dict], page_num: int, db_columns: list[str]):
        """Convert a page of Airtable records to a DataFrame."""
        data: list[dict] = []
        parsing_errors: list[dict] = []

        for record in page:
            fields = record["fields"]

            product_line = fields.get("Product Line", "")
            rate_change_raw = fields.get("Overall Rate Change Number")
            rate_change_parsed = self._parse_number(rate_change_raw, "Overall Rate Change Number")

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
                "Product_Line": product_line,
                "Rate_Change_Type": fields.get("Rate Change Type", ""),
                "Premium_Change_Number": rate_change_parsed,
                "Premium_Change_Amount_Text": fields.get("Overall Rate Change", ""),
                "Effective_Date": self._parse_date(
                    fields.get("Effective Date")
                    or fields.get("Effective Date (New)")
                    or fields.get("Effective Date Requested (New)")
                ),
                "Previous_Increase_Date": self._parse_date(fields.get("Previous Increase Date")),
                "Previous_Increase_Number": self._parse_number(
                    fields.get("Previous Increase Number"), "Previous Increase Number"
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

        if parsing_errors:
            logger.warning(f"Page {page_num}: {len(parsing_errors)} records with parsing errors")

        df = pd.DataFrame(data)

        df_columns = [col for col in df.columns if col in db_columns]
        df_filtered = df[df_columns].reset_index(drop=True)

        logger.info(f"Fetched page {page_num} with {len(df_filtered)} records")

        return df_filtered, len(parsing_errors)

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

        try:
            records = self._safe_call(self.table.all)[:sample_size]
        except RetryError as exc:
            logger.error(f"Failed to fetch diagnostic records: {exc}")
            raise

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
        try:
            records = self._safe_call(self.table.all)[:limit]
        except RetryError as exc:
            logger.error(f"Failed to fetch debug records: {exc}")
            raise

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

        try:
            records = self._safe_call(self.table.all)[:records_sample]
        except RetryError as exc:
            logger.error(f"Failed to inspect percentage field: {exc}")
            raise

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
            "Previous Increase Number",
            "Policyholders Affected Number",
            "Total Written Premium Number",
            "Impact Score",
        ]

        logger.info("=== Validating All Numeric Fields ===")
        try:
            records = self._safe_call(self.table.all)
        except RetryError as exc:
            logger.error(f"Failed to validate numeric fields: {exc}")
            raise

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
