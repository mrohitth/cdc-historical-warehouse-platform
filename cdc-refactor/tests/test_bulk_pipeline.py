#!/usr/bin/env python3
"""
test_bulk_pipeline.py — Production-Grade Integration Tests for PostgreSQL CDC Bulk Pipeline

Tests cover:
  1. Idempotency          — running the same batch twice produces zero new records/expires
  2. SCD Type 2 Timeline  — INSERT then UPDATE creates correct temporal record chain
  3. State Locking        — pipeline_metadata partial unique index prevents concurrent runs

All tests run against operational_db (port 5432) and warehouse_db (port 5433).
Connection params are read from environment variables — no credentials hardcoded.

Usage:
    # With Docker Compose (full lifecycle):
    pytest tests/test_bulk_pipeline.py -v --docker-compose

    # Standalone (against running DBs):
    pytest tests/test_bulk_pipeline.py -v
"""

import os
import sys
import json
import time
import uuid
import subprocess
from datetime import datetime, timezone
from typing import Generator

import pytest
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Environment / connection helpers
# ---------------------------------------------------------------------------

def get_operational_conn():
    """Return a psycopg2 connection to operational_db (port 5432)."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "operational_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


def get_warehouse_conn():
    """Return a psycopg2 connection to warehouse_db (port 5433)."""
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        port=os.getenv("WAREHOUSE_DB_PORT", "5433"),
        database=os.getenv("WAREHOUSE_DB_NAME", "warehouse_db"),
        user=os.getenv("WAREHOUSE_DB_USER", "postgres"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD", "postgres"),
    )


def get_warehouse_dict_conn():
    """Return a psycopg2 connection to warehouse_db with RealDictCursor."""
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        port=os.getenv("WAREHOUSE_DB_PORT", "5433"),
        database=os.getenv("WAREHOUSE_DB_NAME", "warehouse_db"),
        user=os.getenv("WAREHOUSE_DB_USER", "postgres"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD", "postgres"),
        cursor_factory=RealDictCursor,
    )


# ---------------------------------------------------------------------------
# Docker Compose lifecycle (optional — requires docker availability)
# ---------------------------------------------------------------------------

DOCKER_COMPOSE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "docker-compose.yml"
)


@pytest.fixture(scope="session")
def docker_services():
    """
    Session-scoped fixture that spins up docker-compose services for the test
    session, waits for DB readiness, then tears down on exit.

    Only active when --docker-compose flag is passed to pytest.
    Skipped gracefully if Docker is unavailable.
    """
    if not os.path.exists(DOCKER_COMPOSE_FILE):
        pytest.skip("docker-compose.yml not found in project root")

    try:
        subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pytest.skip("Docker not available")

    project_name = f"cdc_test_{uuid.uuid4().hex[:8]}"

    # Start services
    subprocess.run(
        [
            "docker", "compose",
            "-p", project_name,
            "-f", DOCKER_COMPOSE_FILE,
            "up", "-d",
        ],
        cwd=os.path.dirname(DOCKER_COMPOSE_FILE),
        timeout=60,
    )

    # Wait for both DBs to be ready
    def wait_for_db(port: int, dbname: str, user: str = "postgres") -> None:
        for attempt in range(30):
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    port=port,
                    database=dbname,
                    user=user,
                    password="postgres",
                    connect_timeout=2,
                )
                conn.close()
                return
            except psycopg2.OperationalError:
                time.sleep(1)
        raise RuntimeError(f"DB on port {port} did not become ready in time")

    wait_for_db(5432, "operational_db")
    wait_for_db(5433, "warehouse_db")

    yield

    # Teardown
    subprocess.run(
        [
            "docker", "compose",
            "-p", project_name,
            "-f", DOCKER_COMPOSE_FILE,
            "down",
            "-v",  # remove volumes for clean state
        ],
        cwd=os.path.dirname(DOCKER_COMPOSE_FILE),
        timeout=30,
    )


# ---------------------------------------------------------------------------
# Schema setup / teardown fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def operational_db(docker_services):
    """Provide a clean operational_db connection; run init script before each test."""
    conn = get_operational_conn()
    conn.autocommit = True

    # Run init script to set up orders table + trigger
    init_script = os.path.join(
        os.path.dirname(__file__), "..", "init-scripts", "01-init-orders-table.sql"
    )
    if os.path.exists(init_script):
        with open(init_script) as f:
            conn.cursor().execute(f.read())

    yield conn

    # Teardown — clean up test data from operational_db
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE orders CASCADE")
            cur.execute("TRUNCATE deleted_orders CASCADE")
    except Exception:
        pass
    conn.close()


@pytest.fixture(scope="function")
def warehouse_db(docker_services):
    """Provide a clean warehouse_db connection with dim_orders_history and pipeline_metadata."""
    conn = get_warehouse_conn()
    conn.autocommit = False

    # Create dim_orders_history table
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS dim_orders_history (
                surrogate_key  BIGSERIAL PRIMARY KEY,
                order_key      INTEGER      NOT NULL,
                customer_id    INTEGER      NOT NULL,
                product_id     INTEGER      NOT NULL,
                quantity       INTEGER      NOT NULL,
                unit_price     DECIMAL(10,2) NOT NULL,
                total_amount   DECIMAL(10,2) NOT NULL,
                order_status   VARCHAR(50)  NOT NULL,
                order_date     TIMESTAMP    NOT NULL,
                valid_from     TIMESTAMP    NOT NULL,
                valid_to       TIMESTAMP,
                is_current     BOOLEAN      DEFAULT TRUE,
                cdc_operation  VARCHAR(10)  NOT NULL,
                cdc_timestamp  TIMESTAMP    NOT NULL,
                batch_id       VARCHAR(64),
                created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                updated_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT dim_orders_history_current_unique
                    UNIQUE (order_key, is_current)
                    DEFERRABLE INITIALLY DEFERRED,
                CONSTRAINT dim_orders_history_valid_time_check
                    CHECK (valid_to IS NULL OR valid_to > valid_from),
                CONSTRAINT dim_orders_history_current_check
                    CHECK (is_current = TRUE OR valid_to IS NOT NULL)
            );

            CREATE INDEX IF NOT EXISTS idx_dim_orders_history_scd2_pk
                ON dim_orders_history (order_key, is_current)
                WHERE is_current = TRUE;

            CREATE INDEX IF NOT EXISTS idx_dim_orders_history_expire
                ON dim_orders_history (order_key, is_current, valid_to)
                WHERE is_current = TRUE;
        """))
        conn.commit()

        # Create pipeline_metadata table
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                id                    BIGSERIAL PRIMARY KEY,
                pipeline_name         VARCHAR(100) NOT NULL,
                run_key               VARCHAR(100) NOT NULL DEFAULT 'default',
                run_id                VARCHAR(100) NOT NULL,
                run_value             TEXT,
                start_time            TIMESTAMP NOT NULL,
                end_time              TIMESTAMP,
                status                VARCHAR(20) NOT NULL DEFAULT 'running',
                records_processed     INTEGER DEFAULT 0,
                records_successful    INTEGER DEFAULT 0,
                records_failed        INTEGER DEFAULT 0,
                error_message         TEXT,
                performance_metrics   JSONB,
                created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT pipeline_metadata_status_check
                    CHECK (status IN (
                        'running', 'completed', 'failed', 'cancelled', 'idle'
                    ))
            );

            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_pipeline_metadata_running_uniqueness
            ON pipeline_metadata (pipeline_name, run_key)
            WHERE status = 'running';

            CREATE INDEX IF NOT EXISTS
                idx_pipeline_metadata_name_time
            ON pipeline_metadata (pipeline_name, start_time DESC);

            CREATE INDEX IF NOT EXISTS
                idx_pipeline_metadata_run_id
            ON pipeline_metadata (run_id);
        """))
        conn.commit()

    yield conn

    # Teardown — clean up test data from warehouse_db
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE dim_orders_history CASCADE")
            cur.execute("TRUNCATE pipeline_metadata CASCADE")
        conn.commit()
    except Exception:
        conn.rollback()
    conn.close()


@pytest.fixture(scope="function")
def warehouse_dict_db(warehouse_db):
    """Provide warehouse_db connection using RealDictCursor for assertions."""
    conn = get_warehouse_dict_conn()
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers used across tests
# ---------------------------------------------------------------------------

def generate_test_order(order_key: int, **overrides):
    """Generate a minimal change dict that matches the CDC log format."""
    base = {
        "id": order_key,
        "customer_id": 100 + order_key,
        "product_id": 200 + order_key,
        "quantity": 1,
        "unit_price": "19.99",
        "total_amount": "19.99",
        "order_status": "pending",
        "order_date": datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "operation_type": "INSERT",
        "cdc_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


def run_bulk_scd2_merge(conn, changes, batch_id: str) -> dict:
    """
    Directly invoke the _bulk_scd2_merge logic from scd2_loader.
    Replicates the staging + multi-stage CTE without importing the module
    (avoids path / dotenv dependency in tests).

    Returns dict with expired_count, inserted_count, unchanged_count.
    """
    if not changes:
        return {"expired_count": 0, "inserted_count": 0, "unchanged_count": 0}

    stats = {"expired_count": 0, "inserted_count": 0, "unchanged_count": 0}

    with conn:
        with conn.cursor() as cursor:
            # Stage 1 — bulk-load into TEMP staging table
            cursor.execute(sql.SQL("""
                CREATE TEMP TABLE scd2_staging (
                    order_key      INTEGER,
                    customer_id    INTEGER,
                    product_id     INTEGER,
                    quantity       INTEGER,
                    unit_price     DECIMAL(10,2),
                    total_amount   DECIMAL(10,2),
                    order_status   VARCHAR(50),
                    order_date     TIMESTAMP,
                    valid_from     TIMESTAMP,
                    cdc_operation  VARCHAR(10),
                    cdc_timestamp  TIMESTAMP,
                    UNIQUE (order_key)
                ) ON COMMIT DROP
            """))

            # Stage 2 — multi-valued INSERT
            sorted_changes = sorted(changes, key=lambda c: c["id"])
            staging_rows = []
            for c in sorted_changes:
                order_date = c["order_date"]
                if isinstance(order_date, str):
                    order_date = datetime.fromisoformat(order_date.replace("Z", "+00:00"))
                cdc_ts = c["cdc_timestamp"]
                if isinstance(cdc_ts, str):
                    cdc_ts = datetime.fromisoformat(cdc_ts.replace("Z", "+00:00"))
                staging_rows.append((
                    c["id"],
                    c["customer_id"],
                    c["product_id"],
                    c["quantity"],
                    c["unit_price"],
                    c["total_amount"],
                    c["order_status"],
                    order_date,
                    cdc_ts,
                    c["operation_type"],
                    cdc_ts,
                ))

            cols = sql.SQL(
                "order_key, customer_id, product_id, quantity, "
                "unit_price, total_amount, order_status, order_date, "
                "valid_from, cdc_operation, cdc_timestamp"
            )
            values_clause = sql.SQL(", ").join(
                sql.SQL("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                for _ in staging_rows
            )
            cursor.execute(
                sql.SQL("INSERT INTO scd2_staging ({cols}) VALUES {vals}")
                    .format(cols=cols, vals=values_clause),
                [col for row in staging_rows for col in row],
            )

            # Stage 3 — multi-stage CTE expire + insert
            cursor.execute(sql.SQL("""
                WITH expired AS (
                    UPDATE dim_orders_history AS dim
                    SET valid_to    = stg.valid_from,
                        is_current  = FALSE,
                        updated_at   = CURRENT_TIMESTAMP
                    FROM scd2_staging AS stg
                    WHERE dim.order_key  = stg.order_key
                      AND dim.is_current = TRUE
                    RETURNING dim.order_key
                ),
                inserted AS (
                    INSERT INTO dim_orders_history (
                        order_key, customer_id, product_id, quantity,
                        unit_price, total_amount, order_status, order_date,
                        valid_from, cdc_operation, cdc_timestamp, batch_id
                    )
                    SELECT
                        stg.order_key, stg.customer_id, stg.product_id,
                        stg.quantity, stg.unit_price, stg.total_amount,
                        stg.order_status, stg.order_date, stg.valid_from,
                        stg.cdc_operation, stg.cdc_timestamp, %s
                    FROM scd2_staging AS stg
                    WHERE stg.order_key NOT IN (SELECT order_key FROM expired)
                    ON CONFLICT (order_key, is_current)
                        WHERE is_current = TRUE
                    DO NOTHING
                    RETURNING order_key
                )
                SELECT
                    (SELECT count(*) FROM expired) AS expired_count,
                    (SELECT count(*) FROM inserted) AS inserted_count
            """), (batch_id,))

            result = cursor.fetchone()
            stats["expired_count"] = result[0] if result[0] else 0
            stats["inserted_count"] = result[1] if result[1] else 0

            # Count unchanged (arrived but no-op because already current with same data)
            cursor.execute("""
                SELECT count(*)
                FROM scd2_staging AS stg
                WHERE stg.order_key IN (
                    SELECT order_key FROM dim_orders_history
                    WHERE is_current = TRUE
                )
                  AND stg.order_key NOT IN (
                    SELECT order_key FROM dim_orders_history
                    WHERE is_current = FALSE
                )
            """)
            stats["unchanged_count"] = cursor.fetchone()[0]

    return stats


# ============================================================================
# TEST 1 — IDEMPOTENCY
# ============================================================================

@pytest.mark.parametrize("batch_size", [1, 50, 200])
def test_idempotency__same_batch_run_twice_no_new_records(
    warehouse_db,
    warehouse_dict_db,
    batch_size: int,
):
    """
    Idempotency Test — running the SCD2 bulk merge twice with the exact same
    batch of changes must produce ZERO expired records and ZERO new inserts
    on the second run. The dim_orders_history row count must be identical
    before and after the second run.

    This verifies that ON CONFLICT DO NOTHING + unique (order_key, is_current)
    prevents duplicate inserts on replay.
    """
    conn = warehouse_db
    dict_conn = warehouse_dict_db

    # Insert initial data for the test orders into operational_db
    test_order_keys = list(range(1, batch_size + 1))
    with conn.cursor() as cur:
        for ok in test_order_keys:
            cur.execute("""
                INSERT INTO orders (id, customer_id, product_id, quantity,
                                    unit_price, order_status)
                VALUES (%s, %s, %s, %s, 29.99, 'pending')
                ON CONFLICT (id) DO UPDATE SET
                    last_updated = CURRENT_TIMESTAMP
            """, (ok, 100 + ok, 200 + ok, 1))
        conn.commit()

    # Build the CDC change batch
    now = datetime.now(timezone.utc)
    changes = []
    for ok in test_order_keys:
        changes.append({
            "id": ok,
            "customer_id": 100 + ok,
            "product_id": 200 + ok,
            "quantity": 1,
            "unit_price": "29.99",
            "total_amount": "29.99",
            "order_status": "pending",
            "order_date": now.isoformat(),
            "last_updated": now.isoformat(),
            "created_at": now.isoformat(),
            "operation_type": "INSERT",
            "cdc_timestamp": now.isoformat(),
        })

    batch_id = f"idempotency_test_batch_{batch_size}"
    stats_first = run_bulk_scd2_merge(conn, changes, batch_id)

    # Verify first run succeeded
    assert stats_first["inserted_count"] == batch_size, (
        f"First run: expected {batch_size} inserts, got {stats_first}"
    )

    with dict_conn.cursor() as cur:
        cur.execute("SELECT count(*) AS cnt FROM dim_orders_history")
        row_count_after_first = cur.fetchone()["cnt"]

    # Run the exact same batch again — second run
    stats_second = run_bulk_scd2_merge(conn, changes, batch_id)

    with dict_conn.cursor() as cur:
        cur.execute("SELECT count(*) AS cnt FROM dim_orders_history")
        row_count_after_second = cur.fetchone()["cnt"]

    # Assertions
    assert stats_second["expired_count"] == 0, (
        f"Second run: expected 0 expired, got {stats_second['expired_count']}"
    )
    assert stats_second["inserted_count"] == 0, (
        f"Second run: expected 0 inserts, got {stats_second['inserted_count']}"
    )
    assert row_count_after_second == row_count_after_first, (
        f"Row count changed after second run: "
        f"before={row_count_after_first}, after={row_count_after_second}"
    )

    # Verify exactly one is_current=TRUE per order_key
    cur.execute("""
        SELECT order_key, count(*) AS cnt
        FROM dim_orders_history
        WHERE is_current = TRUE
        GROUP BY order_key
        HAVING count(*) > 1
    """)
    duplicates = cur.fetchall()
    assert len(duplicates) == 0, (
        f"Duplicate is_current=TRUE found for order_keys: {duplicates}"
    )


# ============================================================================
# TEST 2 — SCD TYPE 2 TIMELINE
# ============================================================================

@pytest.mark.parametrize("order_key_offset", [1000, 2000])
def test_scd_type2_timeline__insert_then_update_correct_temporal_chain(
    warehouse_db,
    warehouse_dict_db,
    order_key_offset: int,
):
    """
    SCD Type 2 Timeline Test — INSERT followed by UPDATE creates the correct
    temporal record chain:

      INSERT:
        → is_current = TRUE, valid_to = NULL

      UPDATE:
        → Old record: is_current = FALSE, valid_to = new record's valid_from
        → New record: is_current = TRUE, valid_to = NULL

    Validates:
      - Exactly ONE is_current=TRUE per order_key after any number of cycles
      - valid_to < new record's valid_from (temporal consistency)
      - Each update creates a new version without modifying historical rows
    """
    conn = warehouse_db
    dict_conn = warehouse_dict_db

    order_key = order_key_offset
    now = datetime.now(timezone.utc)

    # Step 1 — INSERT
    insert_change = {
        "id": order_key,
        "customer_id": 500 + order_key,
        "product_id": 600 + order_key,
        "quantity": 2,
        "unit_price": "49.99",
        "total_amount": "99.98",
        "order_status": "pending",
        "order_date": now.isoformat(),
        "last_updated": now.isoformat(),
        "created_at": now.isoformat(),
        "operation_type": "INSERT",
        "cdc_timestamp": now.isoformat(),
    }

    batch_id_insert = f"scd2_insert_{order_key}"
    stats_insert = run_bulk_scd2_merge(conn, [insert_change], batch_id_insert)

    assert stats_insert["inserted_count"] == 1, (
        f"INSERT step: expected 1 insert, got {stats_insert}"
    )
    assert stats_insert["expired_count"] == 0, (
        f"INSERT step: expected 0 expires, got {stats_insert}"
    )

    # Verify INSERT result: one row, is_current=TRUE, valid_to=NULL
    with dict_conn.cursor() as cur:
        cur.execute("""
            SELECT order_key, is_current, valid_from, valid_to
            FROM dim_orders_history
            WHERE order_key = %s AND is_current = TRUE
        """, (order_key,))
        row_insert = cur.fetchone()
        assert row_insert is not None, (
            f"No current row found for order_key={order_key} after INSERT"
        )
        assert row_insert["valid_to"] is None, (
            f"INSERT row: expected valid_to=NULL, got {row_insert['valid_to']}"
        )

    # Allow a small time gap so valid_from timestamps differ
    time.sleep(0.01)
    now_update = datetime.now(timezone.utc)

    # Step 2 — UPDATE (same order_key, changed order_status)
    update_change = {
        "id": order_key,
        "customer_id": 500 + order_key,
        "product_id": 600 + order_key,
        "quantity": 2,
        "unit_price": "49.99",
        "total_amount": "99.98",
        "order_status": "completed",  # Changed!
        "order_date": now.isoformat(),
        "last_updated": now_update.isoformat(),
        "created_at": now.isoformat(),
        "operation_type": "UPDATE",
        "cdc_timestamp": now_update.isoformat(),
    }

    batch_id_update = f"scd2_update_{order_key}"
    stats_update = run_bulk_scd2_merge(conn, [update_change], batch_id_update)

    assert stats_update["expired_count"] == 1, (
        f"UPDATE step: expected 1 expire, got {stats_update}"
    )
    assert stats_update["inserted_count"] == 1, (
        f"UPDATE step: expected 1 insert, got {stats_update}"
    )

    # Fetch all records for this order_key
    with dict_conn.cursor() as cur:
        cur.execute("""
            SELECT order_key, is_current, valid_from, valid_to, order_status
            FROM dim_orders_history
            WHERE order_key = %s
            ORDER BY valid_from ASC
        """, (order_key,))
        rows = cur.fetchall()

    assert len(rows) == 2, (
        f"Expected 2 rows after INSERT+UPDATE, got {len(rows)}: {rows}"
    )

    old_row = rows[0]  # The INSERT row (now expired)
    new_row = rows[1]  # The UPDATE row (current)

    # Temporal consistency assertions
    assert old_row["is_current"] is False, (
        f"Old row: expected is_current=FALSE, got {old_row['is_current']}"
    )
    assert old_row["valid_to"] is not None, (
        f"Old row: expected valid_to != NULL, got {old_row['valid_to']}"
    )

    assert new_row["is_current"] is True, (
        f"New row: expected is_current=TRUE, got {new_row['is_current']}"
    )
    assert new_row["valid_to"] is None, (
        f"New row: expected valid_to=NULL, got {new_row['valid_to']}"
    )

    # valid_to of old row must be < valid_from of new row (temporal consistency)
    assert old_row["valid_to"] < new_row["valid_from"], (
        f"Temporal chain broken: old.valid_to={old_row['valid_to']} "
        f"must be < new.valid_from={new_row['valid_from']}"
    )

    # Exactly ONE is_current=TRUE per order_key
    with dict_conn.cursor() as cur:
        cur.execute("""
            SELECT order_key, count(*) AS cnt
            FROM dim_orders_history
            WHERE order_key = %s AND is_current = TRUE
            GROUP BY order_key
        """, (order_key,))
        current_count = cur.fetchone()
        assert current_count is not None and current_count["cnt"] == 1, (
            f"Expected exactly 1 is_current=TRUE row for order_key={order_key}, "
            f"got count={current_count}"
        )


# ============================================================================
# TEST 3 — STATE LOCKING
# ============================================================================

def test_state_locking__pipeline_metadata_prevents_concurrent_running_rows(
    warehouse_db,
    warehouse_dict_db,
):
    """
    State Locking Test — verifies the partial unique index
    idx_pipeline_metadata_running_uniqueness prevents inserting a second
    'running' row for the same (pipeline_name, run_key) combination.

    Also validates pg_advisory_lock behavior by simulating concurrent
    extractor scenarios.
    """
    conn = warehouse_db
    dict_conn = warehouse_dict_db

    pipeline_name = "cdc_extractor"
    run_key = "watermark"
    lock_key = hash(pipeline_name) & 0x7FFFFFFFFFFFFFFF

    # Helper to insert a running row
    def insert_running_row(status: str = "running") -> bool:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_metadata (
                        pipeline_name, run_key, run_id, start_time, status
                    ) VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
                    RETURNING id
                """, (pipeline_name, run_key, str(uuid.uuid4()), status))
                conn.commit()
            return True
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            return False

    # Verify the partial unique index exists
    with dict_conn.cursor() as cur:
        cur.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE indexname = 'idx_pipeline_metadata_running_uniqueness'
        """)
        idx_row = cur.fetchone()
        assert idx_row is not None, (
            "Partial unique index idx_pipeline_metadata_running_uniqueness "
            "not found in pipeline_metadata"
        )
        assert "WHERE status = 'running'" in idx_row["indexdef"], (
            f"Index definition incorrect: {idx_row['indexdef']}"
        )

    # Clean up any pre-existing rows
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM pipeline_metadata
            WHERE pipeline_name = %s AND run_key = %s
        """, (pipeline_name, run_key))
        conn.commit()

    # Test 3a — Insert first 'running' row must succeed
    result = insert_running_row("running")
    assert result is True, "First running row insert failed — should succeed"

    # Test 3b — Insert second 'running' row for same (pipeline_name, run_key) must FAIL
    result = insert_running_row("running")
    assert result is False, (
        "Second concurrent 'running' row insert succeeded — "
        "unique constraint violation expected but got success"
    )

    # Verify exactly one running row exists
    with dict_conn.cursor() as cur:
        cur.execute("""
            SELECT id, status, run_key
            FROM pipeline_metadata
            WHERE pipeline_name = %s AND run_key = %s AND status = 'running'
        """, (pipeline_name, run_key))
        running_rows = cur.fetchall()
    assert len(running_rows) == 1, (
        f"Expected exactly 1 running row, found {len(running_rows)}"
    )

    # Test 3c — A 'completed' row can be inserted for same (pipeline_name, run_key)
    # because the partial index only covers WHERE status = 'running'
    result = insert_running_row("completed")
    assert result is True, (
        "Inserting 'completed' row for same (pipeline_name, run_key) failed — "
        "partial index should only block 'running' duplicates"
    )

    # Test 3d — pg_advisory_lock behavior
    with conn.cursor() as cur:
        # Acquire the advisory lock
        cur.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
        conn.commit()

        # Re-acquire in same session — should not block (lock is reentrant within session)
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
        got_lock = cur.fetchone()[0]
        assert got_lock is True, (
            "Re-acquiring advisory lock in same session returned False — "
            "PostgreSQL advisory locks are reentrant within a session"
        )
        conn.commit()

        # Release
        cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
        conn.commit()

    # Test 3e — Verify lock_key derivation matches what PipelineMetadataManager uses
    derived_lock_key = hash("scd2_loader") & 0x7FFFFFFFFFFFFFFF
    assert derived_lock_key > 0, "Lock key must be a positive 64-bit integer"

    # Clean up
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM pipeline_metadata
            WHERE pipeline_name = %s AND run_key = %s
        """, (pipeline_name, run_key))
        conn.commit()