#!/usr/bin/env python3
"""
CDC Log Extractor for Change Data Capture

This service implements timestamp-based CDC by scanning the operational_db
for changes using a high-watermark approach and writing change logs to JSON files.
"""

import os
import sys
import time
import random
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('cdc_extractor.log')
    ]
)
logger = logging.getLogger(__name__)

class CDCLogExtractor:
    """
    Extracts change data from operational_db using timestamp-based CDC.
    Implements high-watermark tracking for idempotent extraction.
    """
    
    def __init__(self):
        """Initialize database connection and watermark tracking."""
        self.connection = None
        self.watermark_file = Path("data/cdc_logs/.watermark")
        self.cdc_logs_dir = Path("data/cdc_logs")
        self._ensure_directories()
        self._connect()
        
    def _ensure_directories(self) -> None:
        """Ensure necessary directories exist."""
        self.cdc_logs_dir.mkdir(parents=True, exist_ok=True)
        
    def _connect(self) -> None:
        """Establish database connection with retry logic."""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.connection = psycopg2.connect(
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=os.getenv('DB_PORT', '5432'),
                    database=os.getenv('DB_NAME', 'operational_db'),
                    user=os.getenv('DB_USER', 'postgres'),
                    password=os.getenv('DB_PASSWORD', 'postgres')
                )
                self.connection.autocommit = False
                logger.info("Successfully connected to operational_db")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to database after all retries")
                    raise
    
    def _get_watermark(self) -> datetime:
        """
        Get the current high-watermark timestamp.
        Returns the timestamp from file or 5 minutes ago if no watermark exists.
        """
        if self.watermark_file.exists():
            try:
                with open(self.watermark_file, 'r') as f:
                    watermark_str = f.read().strip()
                    return datetime.fromisoformat(watermark_str)
            except (ValueError, IOError) as e:
                logger.warning(f"Failed to read watermark file: {e}")
        
        # Default to 5 minutes ago for initial extraction
        default_watermark = datetime.now(timezone.utc) - timedelta(minutes=5)
        logger.info(f"Using default watermark: {default_watermark}")
        return default_watermark
    
    def _save_watermark(self, timestamp: datetime) -> None:
        """Save the high-watermark timestamp to file."""
        try:
            with open(self.watermark_file, 'w') as f:
                f.write(timestamp.isoformat())
            logger.debug(f"Saved watermark: {timestamp}")
        except IOError as e:
            logger.error(f"Failed to save watermark: {e}")
    
    def _detect_changes(self, since: datetime) -> List[Dict[str, Any]]:
        """
        Detect changes in the orders table since the given timestamp.
        
        Args:
            since: Timestamp to detect changes from
            
        Returns:
            List of change records with operation type and data
        """
        changes = []
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Use a transaction to ensure consistency
                
                # Get current snapshot of all records modified since watermark
                snapshot_query = sql.SQL("""
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
                        'UPSERT' as operation_type
                    FROM orders 
                    WHERE last_updated > %s OR created_at > %s
                    ORDER BY last_updated, id
                """)
                
                cursor.execute(snapshot_query, (since.replace(tzinfo=None), since.replace(tzinfo=None)))
                current_snapshot = cursor.fetchall()
                
                # Get deleted records by comparing with previous snapshot
                # For this demo, we'll use a soft delete approach or track deletions via triggers
                # For now, we'll focus on INSERT/UPDATE operations
                
                self.connection.commit()
                
                # Convert to list of dicts and determine operation types
                for record in current_snapshot:
                    record_dict = dict(record)
                    
                    # Determine if this is an INSERT or UPDATE
                    if record_dict['created_at'] > since.replace(tzinfo=None):
                        record_dict['operation_type'] = 'INSERT'
                    else:
                        record_dict['operation_type'] = 'UPDATE'
                    
                    # Add metadata
                    record_dict['cdc_timestamp'] = datetime.now(timezone.utc).isoformat()
                    record_dict['extracted_at'] = datetime.now(timezone.utc).isoformat()
                    
                    changes.append(record_dict)
                
                logger.info(f"Detected {len(changes)} changes since {since}")
                return changes
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to detect changes: {e}")
            return []
    
    def _write_change_logs(self, changes: List[Dict[str, Any]]) -> None:
        """
        Write changes to JSON log files.
        
        Args:
            changes: List of change records to write
        """
        if not changes:
            return
        
        # Create a log file for this batch
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        log_file = self.cdc_logs_dir / f"changes_{timestamp}.json"
        
        try:
            with open(log_file, 'w') as f:
                json.dump({
                    "batch_metadata": {
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "change_count": len(changes),
                        "watermark": self._get_watermark().isoformat()
                    },
                    "changes": changes
                }, f, indent=2, default=str)
            
            logger.info(f"Wrote {len(changes)} changes to {log_file}")
            
            # Also append to a running log file for easier processing
            running_log = self.cdc_logs_dir / "running_changes.jsonl"
            with open(running_log, 'a') as f:
                for change in changes:
                    f.write(json.dumps(change, default=str) + '\n')
                    
        except IOError as e:
            logger.error(f"Failed to write change logs: {e}")
    
    def _cleanup_old_logs(self, retention_hours: int = 24) -> None:
        """
        Clean up old change log files to prevent disk space issues.
        
        Args:
            retention_hours: Number of hours to retain log files
        """
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
            
            for log_file in self.cdc_logs_dir.glob("changes_*.json"):
                if log_file.stat().st_mtime < cutoff_time.timestamp():
                    log_file.unlink()
                    logger.info(f"Cleaned up old log file: {log_file}")
        except OSError as e:
            logger.warning(f"Failed to cleanup old logs: {e}")
    
    def extract_changes(self, interval_seconds: int = 10) -> None:
        """
        Main extraction loop that periodically scans for changes.
        
        Args:
            interval_seconds: Time between extraction scans
        """
        logger.info("Starting CDC extraction loop")
        logger.info(f"Extraction interval: {interval_seconds} seconds")
        
        try:
            while True:
                logger.info("=== Starting CDC extraction batch ===")
                
                # Get current watermark
                watermark = self._get_watermark()
                logger.info(f"Current watermark: {watermark}")
                
                # Detect changes
                changes = self._detect_changes(watermark)
                
                if changes:
                    # Write change logs
                    self._write_change_logs(changes)
                    
                    # Update watermark to the latest change timestamp
                    latest_timestamp = max(
                        datetime.fromisoformat(change['last_updated'].replace('Z', '+00:00') if isinstance(change['last_updated'], str) else change['last_updated'].isoformat())
                        for change in changes
                    )
                    self._save_watermark(latest_timestamp)
                    
                    logger.info(f"Updated watermark to: {latest_timestamp}")
                else:
                    logger.info("No changes detected")
                
                # Cleanup old logs periodically
                if random.randint(1, 10) == 1:  # 10% chance
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
    
    def create_audit_triggers(self) -> None:
        """
        Create database triggers to track DELETE operations.
        This is optional but provides complete CDC coverage.
        """
        try:
            with self.connection.cursor() as cursor:
                # Create deleted_orders table for audit trail
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS deleted_orders (
                        id SERIAL PRIMARY KEY,
                        original_order_id INTEGER NOT NULL,
                        customer_id INTEGER NOT NULL,
                        product_id INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        unit_price DECIMAL(10,2) NOT NULL,
                        total_amount DECIMAL(10,2) NOT NULL,
                        order_status VARCHAR(50) NOT NULL,
                        order_date TIMESTAMP NOT NULL,
                        last_updated TIMESTAMP NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        deletion_reason TEXT
                    )
                """))
                
                # Create trigger function for soft deletes
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
                            OLD.unit_price, OLD.total_amount, OLD.order_status, OLD.order_date,
                            OLD.last_updated, OLD.created_at, 'CDC_DELETION_TRACKING'
                        );
                        RETURN OLD;
                    END;
                    $$ LANGUAGE plpgsql;
                """))
                
                # Create trigger
                cursor.execute(sql.SQL("""
                    DROP TRIGGER IF EXISTS order_deletion_trigger ON orders;
                    CREATE TRIGGER order_deletion_trigger
                        BEFORE DELETE ON orders
                        FOR EACH ROW
                        EXECUTE FUNCTION log_order_deletion();
                """))
                
                self.connection.commit()
                logger.info("Created audit triggers for DELETE tracking")
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to create audit triggers: {e}")

import random

def main():
    """Main entry point for the CDC log extractor."""
    logger.info("Starting CDC Log Extractor")
    
    try:
        extractor = CDCLogExtractor()
        
        # Optionally create audit triggers for complete CDC coverage
        extractor.create_audit_triggers()
        
        # Get interval from environment or use default
        interval = int(os.getenv('CDC_EXTRACTION_INTERVAL_SECONDS', '10'))
        
        extractor.extract_changes(interval_seconds=interval)
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
