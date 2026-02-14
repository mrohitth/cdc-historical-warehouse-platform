#!/usr/bin/env python3
"""
SCD Type 2 Loader for Warehouse

This transformer implements dbt-style SCD Type 2 logic to load change logs
into the dim_orders_history table in the warehouse database.
Ensures idempotent operations with proper record expiration.
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set
from pathlib import Path
import hashlib

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logging_config import setup_logging
from src.utils.signal_handler import GracefulShutdownHandler, DatabaseConnectionManager

# Load environment variables
load_dotenv()

# Logger will be set up in main() after imports are resolved
logger = None

class SCD2Loader:
    """
    Implements SCD Type 2 loading logic for warehouse dimensions.
    Provides idempotent operations with proper record expiration.
    """
    
    def __init__(self):
        """Initialize warehouse connection and prepare schema."""
        global logger
        if logger is None:
            import os
            logger = setup_logging(__name__, log_level=os.getenv('LOG_LEVEL', 'INFO'))
        
        self.warehouse_connection = None
        self.cdc_logs_dir = Path("data/cdc_logs")
        self.processed_log = Path("data/cdc_logs/.processed_files")
        
        # Initialize graceful shutdown
        self.shutdown_handler = GracefulShutdownHandler(__name__)
        self.conn_manager = DatabaseConnectionManager(self.shutdown_handler, __name__)
        
        # Initialize pipeline metadata
        from src.warehouse.pipeline_metadata import PipelineMetadataManager
        self.metadata_manager = PipelineMetadataManager()
        
        self._ensure_directories()
        self._connect()
        self._create_dim_orders_history()
        self.shutdown_handler.start_listening()
        
    def _ensure_directories(self) -> None:
        """Ensure necessary directories exist."""
        self.processed_log.parent.mkdir(parents=True, exist_ok=True)
        
    def _connect(self) -> None:
        """Connect to warehouse database with retry logic."""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.warehouse_connection = psycopg2.connect(
                    host=os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
                    port=os.getenv('WAREHOUSE_DB_PORT', '5433'),
                    database=os.getenv('WAREHOUSE_DB_NAME', 'warehouse_db'),
                    user=os.getenv('WAREHOUSE_DB_USER', 'postgres'),
                    password=os.getenv('WAREHOUSE_DB_PASSWORD', 'postgres')
                )
                self.warehouse_connection.autocommit = False
                self.conn_manager.add_connection(self.warehouse_connection)
                logger.info("Successfully connected to warehouse_db")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to warehouse after all retries")
                    raise

    def _create_dim_orders_history(self) -> None:
        """
        Create the dim_orders_history table with proper SCD Type 2 structure.
        Includes indexes for optimal query performance.
        """
        try:
            with self.warehouse_connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS dim_orders_history (
                        surrogate_key BIGSERIAL PRIMARY KEY,
                        order_key INTEGER NOT NULL,
                        customer_id INTEGER NOT NULL,
                        product_id INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        unit_price DECIMAL(10,2) NOT NULL,
                        total_amount DECIMAL(10,2) NOT NULL,
                        order_status VARCHAR(50) NOT NULL,
                        order_date TIMESTAMP NOT NULL,
                        valid_from TIMESTAMP NOT NULL,
                        valid_to TIMESTAMP,
                        is_current BOOLEAN DEFAULT TRUE,
                        cdc_operation VARCHAR(10) NOT NULL,
                        cdc_timestamp TIMESTAMP NOT NULL,
                        batch_id VARCHAR(64),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        -- Constraints for data integrity
                        CONSTRAINT dim_orders_history_current_unique 
                            UNIQUE (order_key, is_current) 
                            DEFERRABLE INITIALLY DEFERRED,
                        CONSTRAINT dim_orders_history_valid_time_check 
                            CHECK (valid_to IS NULL OR valid_to > valid_from),
                        CONSTRAINT dim_orders_history_current_check 
                            CHECK (is_current = TRUE OR valid_to IS NOT NULL)
                    );
                    
                    -- Create indexes for optimal performance
                    CREATE INDEX IF NOT EXISTS idx_dim_orders_history_order_key 
                        ON dim_orders_history(order_key);
                    CREATE INDEX IF NOT EXISTS idx_dim_orders_history_is_current 
                        ON dim_orders_history(is_current);
                    CREATE INDEX IF NOT EXISTS idx_dim_orders_history_valid_from 
                        ON dim_orders_history(valid_from);
                    CREATE INDEX IF NOT EXISTS idx_dim_orders_history_valid_to 
                        ON dim_orders_history(valid_to);
                    CREATE INDEX IF NOT EXISTS idx_dim_orders_history_cdc_timestamp 
                        ON dim_orders_history(cdc_timestamp);
                    CREATE INDEX IF NOT EXISTS idx_dim_orders_history_batch_id 
                        ON dim_orders_history(batch_id);
                    
                    -- Add comments for documentation
                    COMMENT ON TABLE dim_orders_history IS 'SCD Type 2 dimension table for orders history';
                    COMMENT ON COLUMN dim_orders_history.surrogate_key IS 'Surrogate key for each version';
                    COMMENT ON COLUMN dim_orders_history.order_key IS 'Natural key from source system';
                    COMMENT ON COLUMN dim_orders_history.valid_from IS 'Start of validity period';
                    COMMENT ON COLUMN dim_orders_history.valid_to IS 'End of validity period (NULL for current)';
                    COMMENT ON COLUMN dim_orders_history.is_current IS 'Flag indicating current record';
                    COMMENT ON COLUMN dim_orders_history.cdc_operation IS 'CDC operation: INSERT/UPDATE/DELETE';
                    COMMENT ON COLUMN dim_orders_history.batch_id IS 'Unique identifier for processing batch';
                """))
                
                self.warehouse_connection.commit()
                logger.info("Created dim_orders_history table with indexes")
                
        except psycopg2.Error as e:
            self.warehouse_connection.rollback()
            logger.error(f"Failed to create dim_orders_history table: {e}")
            raise
    
    def _get_processed_files(self) -> Set[str]:
        """Get set of already processed CDC log files."""
        if not self.processed_log.exists():
            return set()
        
        try:
            with open(self.processed_log, 'r') as f:
                return set(line.strip() for line in f if line.strip())
        except IOError:
            return set()
    
    def _mark_file_processed(self, filename: str, batch_id: str) -> None:
        """Mark a CDC log file as processed with batch ID."""
        try:
            with open(self.processed_log, 'a') as f:
                f.write(f"{filename}|{batch_id}\n")
        except IOError as e:
            logger.error(f"Failed to mark file as processed: {e}")
    
    def _generate_batch_id(self, changes: List[Dict[str, Any]]) -> str:
        """Generate unique batch ID based on changes content."""
        content = json.dumps(sorted([c['id'] for c in changes]), sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()
    
    def _get_current_record(self, order_key: int) -> Optional[Dict[str, Any]]:
        """Get current record for an order key."""
        try:
            with self.warehouse_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql.SQL("""
                    SELECT surrogate_key, order_key, customer_id, product_id, quantity,
                           unit_price, total_amount, order_status, order_date,
                           valid_from, valid_to, is_current, cdc_operation, cdc_timestamp
                    FROM dim_orders_history 
                    WHERE order_key = %s AND is_current = TRUE
                    FOR UPDATE
                """), (order_key,))
                
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except psycopg2.Error as e:
            logger.error(f"Failed to get current record for order {order_key}: {e}")
            return None
    
    def _expire_current_record(self, order_key: int, expire_timestamp: datetime) -> bool:
        """
        Expire the current record for an order key.
        
        Args:
            order_key: The order key to expire
            expire_timestamp: Timestamp to set as valid_to
            
        Returns:
            True if a record was expired, False if no current record found
        """
        try:
            with self.warehouse_connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    UPDATE dim_orders_history 
                    SET valid_to = %s, is_current = FALSE, updated_at = CURRENT_TIMESTAMP
                    WHERE order_key = %s AND is_current = TRUE
                    RETURNING surrogate_key
                """), (expire_timestamp, order_key))
                
                result = cursor.fetchone()
                if result:
                    logger.debug(f"Expired record {result[0]} for order {order_key}")
                    return True
                else:
                    logger.debug(f"No current record found to expire for order {order_key}")
                    return False
                    
        except psycopg2.Error as e:
            logger.error(f"Failed to expire current record for order {order_key}: {e}")
            return False
    
    def _insert_new_record(self, change: Dict[str, Any], batch_id: str) -> Optional[int]:
        """
        Insert a new record for an order.
        
        Args:
            change: Change record from CDC log
            batch_id: Unique batch identifier
            
        Returns:
            Surrogate key of inserted record or None if failed
        """
        try:
            with self.warehouse_connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    INSERT INTO dim_orders_history (
                        order_key, customer_id, product_id, quantity,
                        unit_price, total_amount, order_status, order_date,
                        valid_from, cdc_operation, cdc_timestamp, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING surrogate_key
                """), (
                    change['id'],
                    change['customer_id'],
                    change['product_id'],
                    change['quantity'],
                    change['unit_price'],
                    change['total_amount'],
                    change['order_status'],
                    change['order_date'],
                    change['cdc_timestamp'],
                    change['operation_type'],
                    change['cdc_timestamp'],
                    batch_id
                ))
                
                surrogate_key = cursor.fetchone()[0]
                logger.debug(f"Inserted new record {surrogate_key} for order {change['id']}")
                return surrogate_key
                
        except psycopg2.Error as e:
            logger.error(f"Failed to insert new record for order {change['id']}: {e}")
            return None
    
    def _process_insert_change(self, change: Dict[str, Any], batch_id: str) -> bool:
        """
        Process an INSERT change using SCD Type 2 logic.
        
        Args:
            change: Change record from CDC log
            batch_id: Unique batch identifier
            
        Returns:
            True if processed successfully, False otherwise
        """
        order_key = change['id']
        cdc_timestamp = datetime.fromisoformat(change['cdc_timestamp'].replace('Z', '+00:00'))
        
        # Check if current record already exists (idempotency check)
        current_record = self._get_current_record(order_key)
        if current_record:
            # Record already exists, check if this is a duplicate
            if (current_record['cdc_timestamp'] == cdc_timestamp and 
                current_record['cdc_operation'] == 'INSERT'):
                logger.debug(f"Duplicate INSERT detected for order {order_key}, skipping")
                return True
            else:
                # Treat as UPDATE since record already exists
                logger.info(f"INSERT for existing order {order_key}, treating as UPDATE")
                return self._process_update_change(change, batch_id)
        
        # Insert new record
        surrogate_key = self._insert_new_record(change, batch_id)
        if surrogate_key:
            logger.info(f"Processed INSERT for order {order_key} (surrogate_key: {surrogate_key})")
            return True
        else:
            return False
    
    def _process_update_change(self, change: Dict[str, Any], batch_id: str) -> bool:
        """
        Process an UPDATE change using SCD Type 2 logic.
        
        Args:
            change: Change record from CDC log
            batch_id: Unique batch identifier
            
        Returns:
            True if processed successfully, False otherwise
        """
        order_key = change['id']
        cdc_timestamp = datetime.fromisoformat(change['cdc_timestamp'].replace('Z', '+00:00'))
        
        # Get current record
        current_record = self._get_current_record(order_key)
        if not current_record:
            # No current record exists, treat as INSERT
            logger.info(f"UPDATE for non-existent order {order_key}, treating as INSERT")
            return self._process_insert_change(change, batch_id)
        
        # Check if data has actually changed
        has_changes = (
            current_record['customer_id'] != change['customer_id'] or
            current_record['product_id'] != change['product_id'] or
            current_record['quantity'] != change['quantity'] or
            float(current_record['unit_price']) != float(change['unit_price']) or
            current_record['order_status'] != change['order_status'] or
            str(current_record['order_date']) != str(change['order_date'])
        )
        
        if not has_changes:
            logger.debug(f"No actual changes detected for order {order_key}, skipping")
            return True
        
        # Perform expire and insert in a single transaction
        try:
            with self.warehouse_connection:
                with self.warehouse_connection.cursor() as cursor:
                    # Expire current record
                    cursor.execute(sql.SQL("""
                        UPDATE dim_orders_history 
                        SET valid_to = %s, is_current = FALSE, updated_at = CURRENT_TIMESTAMP
                        WHERE order_key = %s AND is_current = TRUE
                        RETURNING surrogate_key
                    """), (cdc_timestamp, order_key))
                    
                    expired_result = cursor.fetchone()
                    if not expired_result:
                        logger.warning(f"No current record found to expire for order {order_key}")
                        return False
                    
                    logger.debug(f"Expired record {expired_result[0]} for order {order_key}")
                    
                    # Insert new record
                    cursor.execute(sql.SQL("""
                        INSERT INTO dim_orders_history (
                            order_key, customer_id, product_id, quantity,
                            unit_price, total_amount, order_status, order_date,
                            valid_from, cdc_operation, cdc_timestamp, batch_id
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        RETURNING surrogate_key
                    """), (
                        change['id'],
                        change['customer_id'],
                        change['product_id'],
                        change['quantity'],
                        change['unit_price'],
                        change['total_amount'],
                        change['order_status'],
                        change['order_date'],
                        cdc_timestamp,
                        change['operation_type'],
                        cdc_timestamp,
                        batch_id
                    ))
                    
                    surrogate_key = cursor.fetchone()[0]
                    logger.info(f"Processed UPDATE for order {order_key} (surrogate_key: {surrogate_key})")
                    return True
                    
        except psycopg2.Error as e:
            logger.error(f"Failed to process UPDATE for order {order_key}: {e}")
            self.warehouse_connection.rollback()
            return False
    
    def _process_delete_change(self, change: Dict[str, Any], batch_id: str) -> bool:
        """
        Process a DELETE change using SCD Type 2 logic.
        
        Args:
            change: Change record from CDC log
            batch_id: Unique batch identifier
            
        Returns:
            True if processed successfully, False otherwise
        """
        order_key = change['id']
        cdc_timestamp = datetime.fromisoformat(change['cdc_timestamp'].replace('Z', '+00:00'))
        
        # Perform expire in a single transaction for consistency
        try:
            with self.warehouse_connection:
                with self.warehouse_connection.cursor() as cursor:
                    # Expire current record
                    cursor.execute(sql.SQL("""
                        UPDATE dim_orders_history 
                        SET valid_to = %s, is_current = FALSE, updated_at = CURRENT_TIMESTAMP
                        WHERE order_key = %s AND is_current = TRUE
                        RETURNING surrogate_key
                    """), (cdc_timestamp, order_key))
                    
                    expired_result = cursor.fetchone()
                    if expired_result:
                        logger.info(f"Processed DELETE for order {order_key}")
                        return True
                    else:
                        logger.warning(f"No current record found to delete for order {order_key}")
                        return True  # Consider this successful as the end state is correct
                        
        except psycopg2.Error as e:
            logger.error(f"Failed to process DELETE for order {order_key}: {e}")
            self.warehouse_connection.rollback()
            return False
    
    def _process_change_record(self, change: Dict[str, Any], batch_id: str) -> bool:
        """
        Process a single change record using appropriate SCD Type 2 logic.
        
        Args:
            change: Change record from CDC log
            batch_id: Unique batch identifier
            
        Returns:
            True if processed successfully, False otherwise
        """
        operation = change['operation_type']
        
        try:
            if operation == 'INSERT':
                return self._process_insert_change(change, batch_id)
            elif operation == 'UPDATE':
                return self._process_update_change(change, batch_id)
            elif operation == 'DELETE':
                return self._process_delete_change(change, batch_id)
            else:
                logger.warning(f"Unknown operation type: {operation}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to process change record: {e}")
            return False
    
    def _process_batch_file(self, batch_file: Path) -> bool:
        """
        Process a single CDC batch file.
        
        Args:
            batch_file: Path to the batch file
            
        Returns:
            True if processed successfully, False otherwise
        """
        logger.info(f"Processing batch file: {batch_file}")
        
        # Convert string to Path if needed
        if isinstance(batch_file, str):
            batch_file = Path(batch_file)
        
        try:
            with open(batch_file, 'r') as f:
                batch_data = json.load(f)
            
            changes = batch_data.get('changes', [])
            if not changes:
                logger.info(f"No changes found in {batch_file}")
                return True
            
            # Generate batch ID for idempotency
            batch_id = self._generate_batch_id(changes)
            
            # Check if already processed
            processed_files = self._get_processed_files()
            for line in processed_files:
                filename, processed_batch_id = line.split('|', 1)
                if filename == batch_file.name and processed_batch_id == batch_id:
                    logger.info(f"Batch {batch_file.name} with ID {batch_id} already processed")
                    return True
            
            # Group changes by order_key to handle rapid updates
            changes_by_order = {}
            for change in changes:
                order_key = change['id']
                if order_key not in changes_by_order:
                    changes_by_order[order_key] = []
                changes_by_order[order_key].append(change)
            
            # Process changes in a transaction
            success_count = 0
            try:
                with self.warehouse_connection:
                    # Process each order's changes, keeping only the latest
                    for order_key, order_changes in changes_by_order.items():
                        # Sort by cdc_timestamp to get the latest change
                        order_changes.sort(key=lambda x: x['cdc_timestamp'])
                        latest_change = order_changes[-1]
                        
                        # Process only the latest change for each order
                        if self._process_change_record(latest_change, batch_id):
                            success_count += 1
                        else:
                            logger.error(f"Failed to process change for order {order_key}")
                            raise Exception("Failed to process change")
                
                # Mark as processed
                self._mark_file_processed(batch_file.name, batch_id)
                logger.info(f"Successfully processed {success_count}/{len(changes_by_order)} unique orders from {batch_file}")
                return True
                
            except Exception as e:
                logger.error(f"Transaction failed for batch {batch_file}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to process batch file {batch_file}: {e}")
            return False
    
    def load_change_logs(self) -> None:
        """
        Load all unprocessed CDC change logs into dim_orders_history.
        Implements complete SCD Type 2 logic with idempotent operations.
        Tracks pipeline metadata for monitoring.
        """
        logger.info("Starting SCD Type 2 loading process")
        
        # Start pipeline metadata tracking
        start_time = datetime.now(timezone.utc)
        pipeline_id = self.metadata_manager.start_pipeline_run(
            pipeline_name="scd2_loader",
            performance_metrics={
                "start_time": start_time.isoformat(),
                "log_directory": str(self.cdc_logs_dir)
            }
        )
        
        try:
            # Get processed files
            processed_files = self._get_processed_files()
            processed_filenames = {line.split('|', 1)[0] for line in processed_files}
            
            # Find unprocessed batch files
            batch_files = sorted(self.cdc_logs_dir.glob("changes_*.json"))
            unprocessed_files = [f for f in batch_files if f.name not in processed_filenames]
            
            if not unprocessed_files:
                logger.info("No unprocessed batch files found")
                
                # Update metadata - no work to do
                self.metadata_manager.update_pipeline_run(
                    pipeline_id=pipeline_id,
                    status="completed",
                    records_processed=0,
                    records_successful=0,
                    records_failed=0,
                    performance_metrics={
                        "duration_seconds": (datetime.now(timezone.utc) - start_time).total_seconds(),
                        "files_processed": 0
                    }
                )
                return
            
            logger.info(f"Found {len(unprocessed_files)} unprocessed batch files")
            
            # Process each batch file
            successful_batches = 0
            failed_batches = 0
            total_records = 0
            successful_records = 0
            
            for batch_file in unprocessed_files:
                if self.shutdown_handler.should_shutdown:
                    logger.info("Shutdown signal received, stopping batch processing")
                    break
                    
                if self._process_batch_file(batch_file):
                    successful_batches += 1
                    # Count records in this batch
                    try:
                        with open(batch_file, 'r') as f:
                            batch_data = json.load(f)
                            batch_records = len(batch_data.get('changes', []))
                            total_records += batch_records
                            successful_records += batch_records
                    except Exception as e:
                        logger.warning(f"Could not count records in {batch_file}: {e}")
                else:
                    failed_batches += 1
            
            # Determine final status
            status = "completed" if failed_batches == 0 else "completed_with_errors"
            
            # Update pipeline metadata
            self.metadata_manager.update_pipeline_run(
                pipeline_id=pipeline_id,
                status=status,
                records_processed=total_records,
                records_successful=successful_records,
                records_failed=total_records - successful_records,
                performance_metrics={
                    "duration_seconds": (datetime.now(timezone.utc) - start_time).total_seconds(),
                    "files_processed": successful_batches,
                    "files_failed": failed_batches,
                    "total_files": len(unprocessed_files)
                }
            )
            
            logger.info(f"SCD Type 2 loading completed: {successful_batches} successful, {failed_batches} failed")
            
            # Log summary statistics
            self._log_summary_statistics()
            
        except Exception as e:
            logger.error(f"Fatal error in SCD Type 2 loading: {e}")
            
            # Update metadata with error
            self.metadata_manager.update_pipeline_run(
                pipeline_id=pipeline_id,
                status="failed",
                error_message=str(e),
                performance_metrics={
                    "duration_seconds": (datetime.now(timezone.utc) - start_time).total_seconds(),
                    "failed_at": datetime.now(timezone.utc).isoformat()
                }
            )
            raise
        finally:
            logger.info("SCD Type 2 loading process finished")
    
    def _log_summary_statistics(self) -> None:
        """Log summary statistics of the dim_orders_history table."""
        try:
            with self.warehouse_connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN is_current = TRUE THEN 1 END) as current_records,
                        COUNT(CASE WHEN is_current = FALSE THEN 1 END) as historical_records,
                        COUNT(DISTINCT order_key) as unique_orders,
                        MIN(valid_from) as earliest_record,
                        MAX(valid_from) as latest_record
                    FROM dim_orders_history
                """))
                
                stats = cursor.fetchone()
                logger.info(f"dim_orders_history summary: "
                           f"total={stats[0]}, current={stats[1]}, "
                           f"historical={stats[2]}, unique_orders={stats[3]}, "
                           f"earliest={stats[4]}, latest={stats[5]}")
                
        except psycopg2.Error as e:
            logger.error(f"Failed to get summary statistics: {e}")

def main():
    """Main entry point for the SCD Type 2 loader."""
    global logger
    logger = setup_logging(__name__, log_level=os.getenv('LOG_LEVEL', 'INFO'))
    logger.info("Starting SCD Type 2 Loader")
    
    try:
        loader = SCD2Loader()
        loader.load_change_logs()
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
