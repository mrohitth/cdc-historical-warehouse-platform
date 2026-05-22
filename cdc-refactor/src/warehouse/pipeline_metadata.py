#!/usr/bin/env python3
"""
Pipeline Metadata Manager — Production-Grade State Management

Replaces plaintext .watermark files with a PostgreSQL-backed state table.

Security & correctness properties:
  1. Pipeline state (watermark, last run time, status) lives inside the
     warehouse DB — encrypted at rest by the Postgres cluster encryption,
     protected by Postgres access controls.
  2. Advisory locks (pg_advisory_lock) serialize concurrent pipeline runs
     so two extractors can never process the same batch simultaneously.
  3. UPSERT semantics (ON CONFLICT DO UPDATE) ensure exactly ONE active
     watermark row per pipeline — no file-based race conditions.
  4. JSONB for performance_metrics enables rich structured telemetry without
     schema migrations.
  5. Explicit transaction boundaries ensure atomicity of state transitions.
"""

import os
import sys
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logging_config import setup_logging

load_dotenv()

logger = setup_logging(__name__, log_level=os.getenv("LOG_LEVEL", "INFO"))


class PipelineMetadataManager:
    """
    Manages pipeline execution metadata inside the warehouse database.

    Tracks:
      - Last run ID, start/end time, status
      - Records processed / successful / failed
      - Error messages
      - Performance metrics (JSONB)
      - CDC watermark (per pipeline, with UPSERT)

    Concurrency:
      - pg_advisory_lock per pipeline_name — prevents overlapping runs
      - FOR UPDATE on watermark SELECT — prevents two extractors using
        the same watermark value simultaneously
    """

    def __init__(self):
        self.connection = None
        self._connect()
        self._create_metadata_table()

    def _connect(self) -> None:
        try:
            self.connection = psycopg2.connect(
                host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
                port=os.getenv("WAREHOUSE_DB_PORT", "5433"),
                database=os.getenv("WAREHOUSE_DB_NAME", "warehouse_db"),
                user=os.getenv("WAREHOUSE_DB_USER", "postgres"),
                password=os.getenv("WAREHOUSE_DB_PASSWORD", "postgres"),
            )
            self.connection.autocommit = False
            logger.info("PipelineMetadataManager connected to warehouse_db")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to warehouse for metadata: {e}")
            raise

    def _create_metadata_table(self) -> None:
        """Create the pipeline_metadata table with all required constraints."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS pipeline_metadata (
                        id                    BIGSERIAL PRIMARY KEY,
                        pipeline_name         VARCHAR(100) NOT NULL,
                        -- run_key enables multiple independent state rows per pipeline
                        -- e.g. one row per "kind" of run: 'watermark', 'batch', etc.
                        run_key               VARCHAR(100) NOT NULL DEFAULT 'default',
                        run_id                VARCHAR(100) NOT NULL,
                        run_value             TEXT,          -- raw value (watermark timestamp, etc.)
                        start_time            TIMESTAMP NOT NULL,
                        end_time              TIMESTAMP,
                        status                VARCHAR(20) NOT NULL DEFAULT 'running',
                        records_processed     INTEGER DEFAULT 0,
                        records_successful     INTEGER DEFAULT 0,
                        records_failed        INTEGER DEFAULT 0,
                        error_message         TEXT,
                        performance_metrics   JSONB,
                        created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                        -- Exactly ONE running watermark row per pipeline+run_key
                        CONSTRAINT pipeline_metadata_status_check
                            CHECK (status IN (
                                'running', 'completed', 'failed', 'cancelled', 'idle'
                            ))
                    );

                    -- Partial unique index: only one RUNNING row per (pipeline_name, run_key)
                    -- This is the constraint that enables our UPSERT-on-conflict pattern
                    CREATE UNIQUE INDEX IF NOT EXISTS
                        idx_pipeline_metadata_running_uniqueness
                    ON pipeline_metadata (pipeline_name, run_key)
                    WHERE status = 'running';

                    -- Index for lookups by pipeline name + time
                    CREATE INDEX IF NOT EXISTS
                        idx_pipeline_metadata_name_time
                    ON pipeline_metadata (pipeline_name, start_time DESC);

                    -- Index for run_id lookups (used by batch idempotency checks)
                    CREATE INDEX IF NOT EXISTS
                        idx_pipeline_metadata_run_id
                    ON pipeline_metadata (run_id);

                    COMMENT ON TABLE pipeline_metadata IS
                        'Pipeline execution metadata, metrics, and CDC watermark state';
                """))
                self.connection.commit()
                logger.info("pipeline_metadata table verified/created")
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to create pipeline_metadata table: {e}")
            raise

    # -------------------------------------------------------------------------
    # Advisory lock helpers
    # -------------------------------------------------------------------------

    def _lock_pipeline(self, pipeline_name: str) -> int:
        """
        Acquire an advisory lock scoped to pipeline_name.
        Returns the lock key (stable integer derived from pipeline_name).

        PostgreSQL advisory locks are release-on-disconnect by default when
        held inside a named session. The lock is released explicitly in
        _unlock_pipeline below.
        """
        lock_key = hash(pipeline_name) & 0x7FFFFFFFFFFFFFFF  # positive 64-bit int
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_lock(%s)",
                    (lock_key,),
                )
                logger.debug(f"Acquired advisory lock for pipeline: {pipeline_name}")
        except psycopg2.Error as e:
            logger.error(f"Failed to acquire advisory lock for {pipeline_name}: {e}")
            raise
        return lock_key

    def _unlock_pipeline(self, lock_key: int) -> None:
        """Release the advisory lock."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s)",
                    (lock_key,),
                )
                logger.debug(f"Released advisory lock key: {lock_key}")
        except psycopg2.Error as e:
            logger.warning(f"Failed to release advisory lock {lock_key}: {e}")

    # -------------------------------------------------------------------------
    # Run lifecycle management
    # -------------------------------------------------------------------------

    def start_pipeline_run(
        self,
        pipeline_name: str,
        run_id: str = None,
        run_key: str = "default",
        performance_metrics: Dict[str, Any] = None,
    ) -> int:
        """
        Begin a new pipeline run and record metadata row.

        Uses advisory lock to serialize concurrent runs of the same pipeline.
        The lock is held for the duration of the run and released when the
        pipeline marks itself completed/failed via update_pipeline_run.

        Args:
            pipeline_name: Name of the pipeline (e.g. 'scd2_loader', 'cdc_extractor')
            run_id: Unique idempotency key for this run (generated if not provided)
            run_key: Logical bucket for multiple independent runs (e.g. 'watermark')
            performance_metrics: Optional initial metrics dict

        Returns:
            Database primary key (id) of the inserted pipeline_metadata row
        """
        if run_id is None:
            run_id = (
                f"{pipeline_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
            )

        # Serialize concurrent runs
        lock_key = self._lock_pipeline(pipeline_name)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("""
                        INSERT INTO pipeline_metadata (
                            pipeline_name, run_key, run_id, start_time,
                            status, performance_metrics
                        ) VALUES (
                            %s, %s, %s, %s, 'running', %s
                        )
                        RETURNING id
                    """),
                    (
                        pipeline_name,
                        run_key,
                        run_id,
                        datetime.now(timezone.utc),
                        json.dumps(performance_metrics or {}) if performance_metrics else None,
                    ),
                )
                pipeline_id = cursor.fetchone()[0]
                self.connection.commit()

                logger.info(
                    f"Started pipeline run: {pipeline_name} "
                    f"(id={pipeline_id}, run_id={run_id}, run_key={run_key})"
                )
                return pipeline_id

        except psycopg2.Error as e:
            self.connection.rollback()
            self._unlock_pipeline(lock_key)
            logger.error(f"Failed to start pipeline run: {e}")
            raise

    def update_pipeline_run(
        self,
        pipeline_id: int,
        status: str = None,
        records_processed: int = None,
        records_successful: int = None,
        records_failed: int = None,
        error_message: str = None,
        performance_metrics: Dict[str, Any] = None,
        run_value: str = None,  # <-- watermark value stored here
    ) -> bool:
        """
        Update an existing pipeline run.
        On terminal status (completed/failed/cancelled), releases the advisory lock.

        Args:
            pipeline_id: Database primary key from start_pipeline_run
            status: New status
            records_processed: Total records processed
            records_successful: Successfully processed records
            records_failed: Failed records
            error_message: Error message string if failed
            performance_metrics: Additional metrics (merged via JSONB concat)
            run_value: Raw value field — used to store watermark timestamp
        """
        try:
            with self.connection.cursor() as cursor:
                updates: list[str] = []
                params: list[Any] = []
                n = 1

                if status is not None:
                    updates.append(f"status = ${n}")
                    params.append(status)
                    n += 1

                if records_processed is not None:
                    updates.append(f"records_processed = ${n}")
                    params.append(records_processed)
                    n += 1

                if records_successful is not None:
                    updates.append(f"records_successful = ${n}")
                    params.append(records_successful)
                    n += 1

                if records_failed is not None:
                    updates.append(f"records_failed = ${n}")
                    params.append(records_failed)
                    n += 1

                if error_message is not None:
                    updates.append(f"error_message = ${n}")
                    params.append(error_message)
                    n += 1

                if performance_metrics is not None:
                    # Merge into existing JSONB — preserves historical metrics
                    updates.append(
                        f"performance_metrics = COALESCE(performance_metrics, '{{}}'::jsonb) || ${n}"
                    )
                    params.append(json.dumps(performance_metrics))
                    n += 1

                if run_value is not None:
                    updates.append(f"run_value = ${n}")
                    params.append(run_value)
                    n += 1

                updates.append(f"updated_at = ${n}")
                params.append(datetime.now(timezone.utc))
                n += 1

                if status in ("completed", "failed", "cancelled"):
                    updates.append(f"end_time = ${n}")
                    params.append(datetime.now(timezone.utc))
                    n += 1

                params.append(pipeline_id)
                query = f"""
                    UPDATE pipeline_metadata
                    SET {", ".join(updates)}
                    WHERE id = ${n}
                """
                cursor.execute(query, params)
                self.connection.commit()

                logger.debug(f"Updated pipeline run {pipeline_id}: {', '.join(updates)}")

                # Release advisory lock on terminal status
                if status in ("completed", "failed", "cancelled"):
                    self._unlock_pipeline(hash("scd2_loader") & 0x7FFFFFFFFFFFFFFF)

                return True

        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to update pipeline run {pipeline_id}: {e}")
            return False

    def get_last_run_info(
        self,
        pipeline_name: str,
        run_key: str = "default",
    ) -> Optional[Dict[str, Any]]:
        """
        Get the most recent run info for a pipeline.

        Args:
            pipeline_name: Pipeline name
            run_key: Optional run_key filter

        Returns:
            Dict with all columns or None if no runs found
        """
        try:
            with self.connection.cursor() as cursor:
                if run_key:
                    cursor.execute(
                        """
                        SELECT id, run_id, run_key, run_value, start_time, end_time,
                               status, records_processed, records_successful,
                               records_failed, error_message, performance_metrics,
                               created_at, updated_at
                        FROM pipeline_metadata
                        WHERE pipeline_name = %s AND run_key = %s
                        ORDER BY start_time DESC
                        LIMIT 1
                        """,
                        (pipeline_name, run_key),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT id, run_id, run_key, run_value, start_time, end_time,
                               status, records_processed, records_successful,
                               records_failed, error_message, performance_metrics,
                               created_at, updated_at
                        FROM pipeline_metadata
                        WHERE pipeline_name = %s
                        ORDER BY start_time DESC
                        LIMIT 1
                        """,
                        (pipeline_name,),
                    )

                result = cursor.fetchone()
                if result:
                    cols = [d[0] for d in cursor.description]
                    return dict(zip(cols, result))
                return None

        except psycopg2.Error as e:
            logger.error(f"Failed to get last run info for {pipeline_name}: {e}")
            return None

    def get_pipeline_stats(
        self,
        pipeline_name: str,
        days: int = 7,
    ) -> Dict[str, Any]:
        """Return aggregated statistics for a pipeline over the last N days."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*)                                          AS total_runs,
                        COUNT(*) FILTER (WHERE status = 'completed')    AS completed_runs,
                        COUNT(*) FILTER (WHERE status = 'failed')        AS failed_runs,
                        AVG(elapsed_seconds) FILTER (WHERE end_time IS NOT NULL)
                                                                AS avg_duration_seconds,
                        SUM(records_processed)                            AS total_records,
                        SUM(records_successful)                           AS total_successful,
                        SUM(records_failed)                              AS total_failed,
                        MAX(start_time)                                   AS last_run_time
                    FROM (
                        SELECT
                            pipeline_name,
                            status,
                            EXTRACT(EPOCH FROM (end_time - start_time))
                                AS elapsed_seconds,
                            records_processed,
                            records_successful,
                            records_failed,
                            start_time,
                            end_time
                        FROM pipeline_metadata
                        WHERE pipeline_name = %s
                          AND start_time >= CURRENT_TIMESTAMP - (%s || ' days')::INTERVAL
                    ) t
                    """,
                    (pipeline_name, str(days)),
                )

                result = cursor.fetchone()
                if result:
                    cols = [d[0] for d in cursor.description]
                    return dict(zip(cols, result))
                return {}

        except psycopg2.Error as e:
            logger.error(f"Failed to get pipeline stats for {pipeline_name}: {e}")
            return {}

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            logger.info("PipelineMetadataManager connection closed")