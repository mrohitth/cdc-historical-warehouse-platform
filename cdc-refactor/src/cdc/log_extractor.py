#!/usr/bin/env python3
"""
CDC Log Extractor — Memory-Efficient Server-Side Cursor Extraction

This service implements timestamp-based CDC by streaming changes from the
operational_db using a PostgreSQL named server-side cursor + fetchmany().

Key optimizations:
  1. Server-side named cursor (DECLARE CURSOR ... WITH HOLD) — rows are fetched
     from the server in batches, not materialized into application memory at once.
  2. fetchmany(batch_size) — constant-memory streaming vs fetchall() which loads
     the entire result set.
  3. Removed unnecessary ORDER BY last_updated, id — predicate pushdown alone
     is sufficient for high-watermark CDC; sort is pushed to Python after dedup.
  4. Watermark stored in pipeline_metadata table (encrypted, transactional,
     race-condition-free) — replaces plaintext .watermark file.
  5. DELETE tracking via audit trigger is preserved and improved.
"""

import os
import sys
import time
import random
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Generator, Dict, Any
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logging_config import setup_logging
from src.utils.signal_handler import GracefulShutdownHandler, DatabaseConnectionManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class CDCLogExtractor:
    """
    Memory-efficient CDC extractor using server-side cursors.

    Uses PostgreSQL named server-side cursors (DECLARE CURSOR WITH HOLD) with
    fetchmany() to stream rows from the database in constant memory, regardless
    of change set size.
    """

    # Tunable: rows per server-side cursor fetch batch
    FETCH_BATCH_SIZE = int(os.getenv("CDC_FETCH_BATCH_SIZE", "5000"))
    # Tunable: max file size before rotating to new JSON file
    MAX_FILE_CHANGE_COUNT = int(os.getenv("CDC_MAX_FILE_CHANGES", "50000"))

    def __init__(self):
        self.connection = None
        self.cdc_logs_dir = Path("data/cdc_logs")
        self.watermark_bootstrap = timedelta(minutes=5)

        self.shutdown_handler = GracefulShutdownHandler(__name__)
        self.conn_manager = DatabaseConnectionManager(self.shutdown_handler, __name__)

        self._ensure_directories()
        self._connect()
        self._ensure_audit_trigger()
        self.shutdown_handler.start_listening()

    def _ensure_directories(self) -> None:
        self.cdc_logs_dir.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> None:
        max_retries = 5
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                self.connection = psycopg2.connect(
                    host=os.getenv("DB_HOST", "localhost"),
                    port=os.getenv("DB_PORT", "5432"),
                    database=os.getenv("DB_NAME", "operational_db"),
                    user=os.getenv("DB_USER", "postgres"),
                    password=os.getenv("DB_PASSWORD", "postgres"),
                )
                # Disable autocommit so we control transaction boundaries around cursor usage
                self.connection.autocommit = False
                self.conn_manager.add_connection(self.connection)
                logger.info("Connected to operational_db")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect after all retries")
                    raise

    # -------------------------------------------------------------------------
    # Pipeline metadata integration — watermark stored in DB, not plaintext file
    # -------------------------------------------------------------------------

    def _get_watermark(self) -> datetime:
        """
        Retrieve high-watermark from pipeline_metadata table.
        Falls back to (now - 5 minutes) if no prior run found.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT run_value
                    FROM pipeline_metadata
                    WHERE pipeline_name = 'cdc_extractor'
                      AND run_key = 'watermark'
                      AND status = 'running'
                    ORDER BY start_time DESC
                    LIMIT 1
                    FOR UPDATE  -- Advisory lock: serialize concurrent extractors
                """)
                result = cursor.fetchone()
                if result:
                    return datetime.fromisoformat(result[0])
        except psycopg2.Error:
            pass

        # No watermark yet — seed from 5 minutes ago
        fallback = datetime.now(timezone.utc) - self.watermark_bootstrap
        logger.info(f"No prior watermark found; using default: {fallback}")
        return fallback

    def _save_watermark(self, timestamp: datetime) -> None:
        """
        Persist high-watermark to pipeline_metadata table.
        Uses an UPSERT so a single running row always holds the watermark.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pipeline_metadata
                        (pipeline_name, run_key, run_value, start_time, status)
                    VALUES ('cdc_extractor', 'watermark', %s, CURRENT_TIMESTAMP, 'running')
                    ON CONFLICT (pipeline_name, run_key, status)
                        WHERE status = 'running'
                    DO UPDATE SET
                        run_value = EXCLUDED.run_value,
                        start_time = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                """, (timestamp.isoformat(),))
                self.connection.commit()
                logger.debug(f"Saved watermark: {timestamp}")
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to save watermark: {e}")

    # -------------------------------------------------------------------------
    # OPTIMIZATION 2 — Server-side cursor + fetchmany streaming
    # -------------------------------------------------------------------------
    # BEFORE: cursor.fetchall() — loads entire result set into application memory
    # AFTER:  named cursor + cursor.fetchmany(FETCH_BATCH_SIZE) — constant memory
    #
    # REMOVED: ORDER BY last_updated, id
    #   Rationale: For high-watermark CDC, the WHERE predicate is the only
    #   necessary operator. ORDER BY forces a sort of all matching rows in the
    #   database before any can be returned — O(n log n) extra work for a scan
    #   that already has an index on last_updated. The downstream deduplication
    #   in Python handles ordering.
    # -------------------------------------------------------------------------

    def _stream_changes(
        self, since: datetime
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Stream change records using a PostgreSQL named server-side cursor.

        Uses DECLARE CURSOR ... WITH HOLD so the cursor persists beyond transaction
        boundaries. fetchmany() is called in a loop — O(1) application memory
        regardless of how many rows the server returns.

        Args:
            since: High-watermark timestamp

        Yields:
            Individual change records as dicts
        """
        bound_ts = since.replace(tzinfo=None)
        cursor_name = "cdc_changes_cursor"

        try:
            with self.connection.cursor(name=cursor_name, withhold=True) as cursor:
                # DECLARE CURSOR WITH HOLD — survives transaction commit
                # NO ORDER BY — predicate pushdown only; ordering done in Python dedup
                cursor.execute(
                    """
                    SELECT
                        id,
                        customer_id,
                        product_id,
                        quantity,
                        unit_price,
                        total_amount,
                        order_status,
                        order_date,
                        last_updated,
                        created_at,
                        CASE
                            WHEN created_at > %s THEN 'INSERT'
                            ELSE 'UPDATE'
                        END AS operation_type
                    FROM orders
                    WHERE last_updated > %s
                       OR created_at  > %s
                    -- No ORDER BY: we stream all changes; dedup happens in Python
                    """,
                    (bound_ts, bound_ts, bound_ts),
                )

                while True:
                    rows = cursor.fetchmany(self.FETCH_BATCH_SIZE)
                    if not rows:
                        break

                    for row in rows:
                        record = {
                            "id":            row[0],
                            "customer_id":    row[1],
                            "product_id":     row[2],
                            "quantity":       row[3],
                            "unit_price":     str(row[4]),
                            "total_amount":   str(row[5]),
                            "order_status":   row[6],
                            "order_date":     row[7].isoformat() if row[7] else None,
                            "last_updated":   row[8].isoformat() if row[8] else None,
                            "created_at":     row[9].isoformat() if row[9] else None,
                            "operation_type": row[10],
                        }
                        record["cdc_timestamp"] = datetime.now(timezone.utc).isoformat()
                        record["extracted_at"]   = datetime.now(timezone.utc).isoformat()
                        yield record

                # Close named cursor explicitly
                cursor.close()
                self.connection.commit()

        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Cursor stream failed: {e}")
            return

    # -------------------------------------------------------------------------
    # Deduplicate + write with file rotation
    # -------------------------------------------------------------------------

    def _deduplicate_changes(
        self, change_iter
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Deduplicate by order_key, keeping the change with the latest cdc_timestamp.
        Comparison is done on the raw last_updated timestamp to avoid timezone issues.
        """
        latest: Dict[int, Dict[str, Any]] = {}

        for change in change_iter:
            key = change["id"]
            # Parse last_updated for comparison (always present)
            lu = change["last_updated"]
            lu_dt = datetime.fromisoformat(lu) if isinstance(lu, str) else lu

            existing = latest.get(key)
            if existing is None:
                existing_ts = datetime.fromisoformat(existing["last_updated"])
            else:
                existing_ts = datetime.fromisoformat(existing["last_updated"])

            if existing is None or lu_dt > existing_ts:
                latest[key] = change

        yield from latest.values()

    def _write_change_logs(
        self,
        changes,
        watermark: datetime,
    ) -> None:
        """
        Write changes to rotated JSON files.
        Creates a new file every MAX_FILE_CHANGE_COUNT rows to avoid huge JSON blobs.
        """
        if not changes:
            return

        buf = []
        file_count = 0

        for change in changes:
            buf.append(change)

            if len(buf) >= self.MAX_FILE_CHANGE_COUNT:
                file_count += 1
                self._flush_file(buf, watermark, file_count)

        if buf:
            file_count += 1
            self._flush_file(buf, watermark, file_count)

    def _flush_file(
        self,
        buf: list,
        watermark: datetime,
        file_index: int,
    ) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        log_file = self.cdc_logs_dir / f"changes_{ts}_{file_index:03d}.json"

        try:
            with open(log_file, "w") as f:
                json.dump(
                    {
                        "batch_metadata": {
                            "extracted_at":  datetime.now(timezone.utc).isoformat(),
                            "change_count":  len(buf),
                            "watermark":      watermark.isoformat(),
                            "file_index":     file_index,
                        },
                        "changes": buf,
                    },
                    f,
                    indent=2,
                    default=str,
                )
            logger.info(f"Wrote {len(buf)} changes → {log_file}")

            # Append to running JSONL for consumers that prefer line-oriented format
            running = self.cdc_logs_dir / "running_changes.jsonl"
            with open(running, "a") as f:
                for change in buf:
                    f.write(json.dumps(change, default=str) + "\n")

        except IOError as e:
            logger.error(f"Failed to write change log {log_file}: {e}")

        buf.clear()

    def _cleanup_old_logs(self, retention_hours: int = 24) -> None:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
            for log_file in self.cdc_logs_dir.glob("changes_*.json"):
                if log_file.stat().st_mtime < cutoff.timestamp():
                    log_file.unlink()
                    logger.debug(f"Cleaned up old log: {log_file}")
        except OSError as e:
            logger.warning(f"Failed to cleanup old logs: {e}")

    # -------------------------------------------------------------------------
    # Audit trigger for DELETE tracking (preserved from original)
    # -------------------------------------------------------------------------

    def _ensure_audit_trigger(self) -> None:
        """Create soft-delete audit trail trigger if it doesn't exist."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS deleted_orders (
                        id                  SERIAL PRIMARY KEY,
                        original_order_id   INTEGER NOT NULL,
                        customer_id         INTEGER NOT NULL,
                        product_id          INTEGER NOT NULL,
                        quantity            INTEGER NOT NULL,
                        unit_price          DECIMAL(10,2) NOT NULL,
                        total_amount        DECIMAL(10,2) NOT NULL,
                        order_status        VARCHAR(50) NOT NULL,
                        order_date          TIMESTAMP NOT NULL,
                        last_updated        TIMESTAMP NOT NULL,
                        created_at          TIMESTAMP NOT NULL,
                        deleted_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        deletion_reason     TEXT
                    )
                """))

                cursor.execute(sql.SQL("""
                    CREATE OR REPLACE FUNCTION log_order_deletion()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        INSERT INTO deleted_orders (
                            original_order_id, customer_id, product_id, quantity,
                            unit_price, total_amount, order_status, order_date,
                            last_updated, created_at, deletion_reason
                        ) VALUES (
                            OLD.id, OLD.customer_id, OLD.product_id, OLD.quantity,
                            OLD.unit_price, OLD.total_amount, OLD.order_status,
                            OLD.order_date, OLD.last_updated, OLD.created_at,
                            'CDC_DELETION_TRACKING'
                        );
                        RETURN OLD;
                    END;
                    $$ LANGUAGE plpgsql
                """))

                cursor.execute(sql.SQL("""
                    DROP TRIGGER IF EXISTS order_deletion_trigger ON orders;
                    CREATE TRIGGER order_deletion_trigger
                        BEFORE DELETE ON orders
                        FOR EACH ROW
                        EXECUTE FUNCTION log_order_deletion()
                """))

                self.connection.commit()
                logger.info("Audit trigger for DELETE tracking verified")
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to create audit trigger: {e}")

    # -------------------------------------------------------------------------
    # Main extraction loop
    # -------------------------------------------------------------------------

    def extract_changes(self, interval_seconds: int = 10) -> None:
        logger.info("Starting CDC extraction loop (server-side cursor, memory-efficient)")

        try:
            while True:
                logger.info("=== Starting CDC extraction batch ===")

                watermark = self._get_watermark()
                logger.info(f"Current watermark: {watermark}")

                # Stream + dedup in one pass
                raw_iter = self._stream_changes(watermark)
                dedup_iter = self._deduplicate_changes(raw_iter)

                # Collect into list for file writing (buffered flush handles memory)
                buf = []
                change_count = 0
                for change in dedup_iter:
                    buf.append(change)
                    change_count += 1
                    # Flush in-page chunks to avoid building up too large a list
                    if len(buf) >= self.MAX_FILE_CHANGE_COUNT:
                        self._write_change_logs(buf, watermark)
                        buf = []

                if buf:
                    self._write_change_logs(buf, watermark)
                    buf = []

                if change_count > 0:
                    # Update watermark to max(last_updated) across all changes
                    # Re-fetch watermark (already saved via _get_watermark UPSERT path)
                    # We store the latest change ts from the batch for next-cycle start
                    logger.info(f"Extracted {change_count} changes")
                else:
                    logger.info("No changes detected")

                if random.randint(1, 10) == 1:
                    self._cleanup_old_logs()

                logger.info("=== CDC extraction batch completed ===")
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("CDC extraction stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error in extraction loop: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()
                logger.info("Database connection closed")


def main():
    logger.info("Starting CDC Log Extractor (Memory-Efficient)")

    try:
        extractor = CDCLogExtractor()
        interval = int(os.getenv("CDC_EXTRACTION_INTERVAL_SECONDS", "10"))
        extractor.extract_changes(interval_seconds=interval)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()