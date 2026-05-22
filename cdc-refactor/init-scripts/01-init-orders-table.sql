-- ============================================================================
-- CDC Historical Warehouse Platform — Production-Grade Schema
-- PostgreSQL 15+
--
-- Changes from original:
--   1. Composite indexes on (last_updated, id) for CDC predicate pushdown
--   2. Partial unique index for SCD2 active-record constraint
--   3. BRIN index on last_updated for time-range scans (very low overhead)
--   4. Optimized column layout for bulk fetch
-- ============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Operational DB: orders source table
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    id            SERIAL PRIMARY KEY,
    customer_id   INTEGER NOT NULL,
    product_id    INTEGER NOT NULL,
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    unit_price    DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    total_amount  DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    order_status  VARCHAR(50) NOT NULL DEFAULT 'pending',
    order_date    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── Indexes for CDC extraction (log_extractor.py) ─────────────────────────────

-- 1. Composite on (last_updated, id) — enables index-only scan for the CDC query:
--    WHERE last_updated > %s OR created_at > %s
--    B-tree can satisfy both predicates from the index without touching the heap.
CREATE INDEX IF NOT EXISTS idx_orders_cdc_scan
    ON orders (last_updated ASC, id ASC);

-- 2. Partial index for new rows only — supports created_at > %s predicate with
--    minimal index size (only new rows, not all rows ever created).
CREATE INDEX IF NOT EXISTS idx_orders_created_recently
    ON orders (created_at DESC)
    WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '7 days';

-- 3. BRIN index on last_updated — extremely low overhead for time-range
--    navigation. For append-mostly data with insertion order ≈ time order,
--    BRIN provides near-zero storage cost and excellent range-scan performance.
CREATE INDEX IF NOT EXISTS idx_orders_last_updated_brin
    ON orders USING BRIN (last_updated)
    WITH (pages_per_range = 32);

-- 4. Unique index on id — ensures no duplicate order IDs
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_id_unique
    ON orders (id);

-- ── Application query indexes ────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date
    ON orders (order_date DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders (order_status);

-- ─────────────────────────────────────────────────────────────────────────────
-- Warehouse DB: SCD Type 2 dimension table
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_orders_history (
    surrogate_key  BIGSERIAL PRIMARY KEY,
    order_key      INTEGER NOT NULL,
    customer_id    INTEGER NOT NULL,
    product_id     INTEGER NOT NULL,
    quantity       INTEGER NOT NULL,
    unit_price     DECIMAL(10,2) NOT NULL,
    total_amount   DECIMAL(10,2) NOT NULL,
    order_status   VARCHAR(50) NOT NULL,
    order_date     TIMESTAMP NOT NULL,
    valid_from     TIMESTAMP NOT NULL,
    valid_to       TIMESTAMP,
    is_current     BOOLEAN DEFAULT TRUE,
    cdc_operation  VARCHAR(10) NOT NULL,
    cdc_timestamp  TIMESTAMP NOT NULL,
    batch_id       VARCHAR(64),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Exactly ONE current record per natural key at any point in time
    -- PostgreSQL's DEFERRABLE INITIALLY DEFERRED delays check to commit time,
    -- allowing the bulk CTE to insert the new version before the uniqueness
    -- check fires, as long as both happen in the same transaction.
    CONSTRAINT dim_orders_history_current_unique
        UNIQUE (order_key, is_current)
        DEFERRABLE INITIALLY DEFERRED,

    -- Temporal integrity: valid_to must be after valid_from (or NULL for current)
    CONSTRAINT dim_orders_history_valid_time_check
        CHECK (valid_to IS NULL OR valid_to > valid_from),

    -- Closed records must have an expiration timestamp
    CONSTRAINT dim_orders_history_current_check
        CHECK (is_current = TRUE OR valid_to IS NOT NULL)
);

-- ── Indexes for SCD2 bulk merge (scd2_loader.py) ──────────────────────────────

-- 1. Primary compound index for SCD2 point lookup
--    Supports WHERE order_key = %s AND is_current = TRUE with index-only scan.
--    Also used as the join key in the bulk CTE UPDATE ... FROM staging.
CREATE INDEX IF NOT EXISTS idx_dim_orders_history_scd2_pk
    ON dim_orders_history (order_key, is_current)
    WHERE is_current = TRUE;

-- 2. Bulk expire index — finds all active records whose keys appear in staging
--    Using ANY(%s) against this index avoids a full-table scan on dim_orders_history.
CREATE INDEX IF NOT EXISTS idx_dim_orders_history_expire
    ON dim_orders_history (order_key, is_current, valid_to)
    WHERE is_current = TRUE;

-- 3. SCD2 lookup — covers (order_key, is_current, valid_from DESC) for
--    point lookups that need temporal ordering without a separate sort.
CREATE INDEX IF NOT EXISTS idx_dim_orders_history_current_lookup
    ON dim_orders_history (order_key, is_current, valid_from DESC)
    WHERE is_current = TRUE;

-- 4. CDC timestamp index (descending) for late-arrival and backfill queries
--    Allows ORDER BY cdc_timestamp DESC LIMIT 1 without a sort.
CREATE INDEX IF NOT EXISTS idx_dim_orders_history_cdc_ts
    ON dim_orders_history (cdc_timestamp DESC);

-- 5. Batch ID index for audit, retry, and cross-batch idempotency checks
CREATE INDEX IF NOT EXISTS idx_dim_orders_history_batch_id
    ON dim_orders_history (batch_id);

-- 6. Composite for valid_from range scans (analytics, effective-as-of queries)
CREATE INDEX IF NOT EXISTS idx_dim_orders_history_valid_range
    ON dim_orders_history (valid_from DESC, order_key);

-- ─────────────────────────────────────────────────────────────────────────────
-- Pipeline metadata table — replaces .watermark files
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id                    BIGSERIAL PRIMARY KEY,
    pipeline_name         VARCHAR(100) NOT NULL,
    run_key               VARCHAR(100) NOT NULL DEFAULT 'default',
    run_id                VARCHAR(100) NOT NULL,
    run_value             TEXT,          -- holds raw values (e.g. watermark ISO string)
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
        CHECK (status IN ('running', 'completed', 'failed', 'cancelled', 'idle'))
);

-- Exactly one RUNNING row per (pipeline_name, run_key) — enables UPSERT pattern
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_metadata_running_uniqueness
    ON pipeline_metadata (pipeline_name, run_key)
    WHERE status = 'running';

-- Time-ordered run history per pipeline
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_name_time
    ON pipeline_metadata (pipeline_name, start_time DESC);

-- Run ID lookups (used for batch idempotency checks in scd2_loader)
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_run_id
    ON pipeline_metadata (run_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- Delete audit trail (populated by trigger)
-- ─────────────────────────────────────────────────────────────────────────────
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
);

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
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS order_deletion_trigger ON orders;
CREATE TRIGGER order_deletion_trigger
    BEFORE DELETE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION log_order_deletion();

-- ─────────────────────────────────────────────────────────────────────────────
-- Sample seed data
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO orders (customer_id, product_id, quantity, unit_price, order_status)
VALUES
    (1,  101, 2, 29.99, 'completed'),
    (2,  102, 1, 49.99, 'pending'),
    (3,  103, 3, 15.99, 'shipped'),
    (4,  104, 1, 199.99, 'completed'),
    (5,  105, 2, 39.99, 'pending')
ON CONFLICT DO NOTHING;