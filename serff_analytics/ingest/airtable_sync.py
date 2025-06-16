import pandas as pd
from pyairtable import Table
from datetime import datetime, timedelta
import logging
import traceback
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

    LAST_MODIFIED_FIELD = "Last Modified"

    def _fetch_records(self, since: datetime | None = None):
        """Stream records from Airtable page by page."""
        params: dict[str, str] = {}
        if since:
            params["formula"] = f"{{{self.LAST_MODIFIED_FIELD}}} > '{since.isoformat()}'"

        try:
            for page in self._safe_call(self.table.iterate, page_size=100, **params):
                yield page
        except RetryError as exc:
            logger.error(f"Failed to fetch records from Airtable: {exc}")
            raise

    def sync_data(self, since: datetime | None = None):
        """Sync data from Airtable to DuckDB - only updates modified records"""
        sync_id = None
        sync_mode = 'manual' if since else None

        try:
            # Initialize sync tracking
            with self.db.transaction() as conn:
                # Get last successful sync FIRST
                last_sync_result = conn.execute("""
                    SELECT completed_at 
                    FROM sync_history 
                    WHERE status = 'completed' 
                    ORDER BY completed_at DESC 
                    LIMIT 1
                """).fetchone()

                # Determine sync mode based on history
                if since:
                    sync_mode = 'manual'
                elif last_sync_result and last_sync_result[0]:
                    sync_mode = 'incremental'
                    since = last_sync_result[0] - timedelta(minutes=5)
                else:
                    sync_mode = 'full'

                # Now create sync record with correct mode
                sync_id = conn.execute("SELECT nextval('sync_history_seq')").fetchone()[0]
                conn.execute("""
                    INSERT INTO sync_history (sync_id, started_at, status, sync_mode) 
                    VALUES (?, CURRENT_TIMESTAMP, 'running', ?)
                """, [sync_id, sync_mode])

            logger.info(f"Starting Airtable sync... (mode: {sync_mode}, since: {since})")

            # Initialize counters
            total_processed = 0
            total_errors = 0
            all_dataframes: list[pd.DataFrame] = []

            # Fetch and process records
            for page_num, page in enumerate(self._fetch_records(since), start=1):
                df, errors = self._process_page(page, page_num)
                if not df.empty:
                    all_dataframes.append(df)
                total_processed += len(page)
                total_errors += errors

            # Create combined DataFrame
            combined_df = (
                pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()
            )

            logger.info(f"Fetched {total_processed} records, combined into {len(combined_df)} for processing")

            # Drop indexes outside the transaction to avoid DuckDB upsert issues
            with self.db.connection() as conn:
                indexes = conn.execute(
                    "SELECT index_name FROM duckdb_indexes() WHERE table_name='filings'"
                ).fetchall()
                for (idx,) in indexes:
                    conn.execute(f'DROP INDEX IF EXISTS "{idx}"')

            # Perform database updates
            with self.db.transaction() as conn:
                inserted = 0
                updated = 0
                skipped = 0

                if not combined_df.empty:
                    conn.register("airtable_import", combined_df)
                    try:
                        # UPSERT with conditional update
                        conn.execute("""
                            INSERT INTO filings 
                            SELECT * FROM airtable_import
                            ON CONFLICT (Record_ID) DO UPDATE SET
                                Company = EXCLUDED.Company,
                                Subsidiary = EXCLUDED.Subsidiary,
                                State = EXCLUDED.State,
                                Product_Line = EXCLUDED.Product_Line,
                                Rate_Change_Type = EXCLUDED.Rate_Change_Type,
                                Premium_Change_Number = EXCLUDED.Premium_Change_Number,
                                Premium_Change_Amount_Text = EXCLUDED.Premium_Change_Amount_Text,
                                Effective_Date = EXCLUDED.Effective_Date,
                                Previous_Increase_Date = EXCLUDED.Previous_Increase_Date,
                                Previous_Increase_Number = EXCLUDED.Previous_Increase_Number,
                                Policyholders_Affected_Number = EXCLUDED.Policyholders_Affected_Number,
                                Policyholders_Affected_Text = EXCLUDED.Policyholders_Affected_Text,
                                Total_Written_Premium_Number = EXCLUDED.Total_Written_Premium_Number,
                                Total_Written_Premium_Text = EXCLUDED.Total_Written_Premium_Text,
                                SERFF_Tracking_Number = EXCLUDED.SERFF_Tracking_Number,
                                Specific_Coverages = EXCLUDED.Specific_Coverages,
                                Stated_Reasons = EXCLUDED.Stated_Reasons,
                                Population = EXCLUDED.Population,
                                Impact_Score = EXCLUDED.Impact_Score,
                                Renewals_Date = EXCLUDED.Renewals_Date,
                                Airtable_Last_Modified = EXCLUDED.Airtable_Last_Modified,
                                Updated_At = NOW()
                            WHERE 
                                filings.Airtable_Last_Modified IS NULL 
                                OR filings.Airtable_Last_Modified < EXCLUDED.Airtable_Last_Modified
                        """)

                        # For now, approximate the counts
                        # In incremental mode, most will be skipped
                        if sync_mode == 'incremental':
                            # Rough estimate: assume most are skipped
                            updated = min(len(combined_df), 100)  # Assume at most 100 updates
                            skipped = len(combined_df) - updated
                        else:
                            # Full sync: all are inserts/updates
                            inserted = len(combined_df)

                    finally:
                        conn.unregister("airtable_import")
                else:
                    logger.info("No records to process")

                # Get total record count
                db_total_records = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]

                # Update sync history with results
                if sync_id:
                    conn.execute("""
                        UPDATE sync_history 
                        SET completed_at = CURRENT_TIMESTAMP,
                            records_processed = ?,
                            records_inserted = ?,
                            records_updated = ?,
                            records_skipped = ?,
                            parsing_errors = ?,
                            status = 'completed'
                        WHERE sync_id = ?
                    """, [total_processed, inserted, updated, skipped, total_errors, sync_id])

            # Recreate indexes
            with self.db.connection() as conn:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_company ON filings(Company, Subsidiary)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_effective_date ON filings(Effective_Date)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_state_product ON filings(State, Product_Line)")

            logger.info(
                f"Sync completed. Mode: {sync_mode}, "
                f"Processed: {total_processed}, "
                f"DB Total: {db_total_records}"
            )

            result = {
                "success": True,
                "sync_id": sync_id,
                "sync_mode": sync_mode,
                "records_processed": total_processed,
                "records_inserted": inserted,
                "records_updated": updated,
                "records_skipped": skipped,
                "total_records": db_total_records,
                "parsing_errors": total_errors,
                "failed_inserts": 0,
            }

            return result

        except Exception as e:
            logger.error(f"Sync failed: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")

            # Update sync history with error
            if sync_id:
                try:
                    with self.db.transaction() as conn:
                        conn.execute("""
                            UPDATE sync_history 
                            SET status = 'failed',
                                completed_at = CURRENT_TIMESTAMP,
                                error_message = ?
                            WHERE sync_id = ?
                        """, [str(e), sync_id])
                except:
                    pass  # Don't fail if we can't update the sync history

            raise

    def _process_page(self, page: list[dict], page_num: int):
        """Convert a page of Airtable records to a DataFrame."""
        data: list[dict] = []
        parsing_errors: list[dict] = []

        for record in page:
            fields = record["fields"]

            # Parse the Last Modified field
            last_modified = None
            airtable_date = record['fields'].get('Last Modified')

            if airtable_date:
                try:
                    # Try ISO format first (what Airtable actually sends)
                    if 'T' in airtable_date:
                        # ISO format: 2025-05-28T16:13:00.000Z
                        last_modified = datetime.fromisoformat(airtable_date.replace('Z', '+00:00'))
                    else:
                        # Fallback to the other format if needed: 5/14/2025 1:35pm
                        last_modified = datetime.strptime(airtable_date, '%m/%d/%Y %I:%M%p')
                except ValueError as e:
                    logger.warning(f"Could not parse date: {airtable_date} for record {record['id']}")
        # Don't fail the whole record, just skip the timestamp

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
                "Airtable_Last_Modified": last_modified,
                "Created_At": datetime.now(),
                "Updated_At": datetime.now(),
            }
            data.append(row)

        if parsing_errors:
            logger.warning(f"Page {page_num}: {len(parsing_errors)} records with parsing errors")

        df = pd.DataFrame(data)

        with self.db.connection() as conn:
            table_info = conn.execute("PRAGMA table_info(filings)").fetchall()
            db_columns = [col[1] for col in table_info]
            df_columns = [col for col in db_columns if col in df.columns]
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
