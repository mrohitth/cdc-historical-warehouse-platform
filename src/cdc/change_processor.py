#!/usr/bin/env python3
"""
Change Data Processor

Processes CDC change logs and prepares them for loading into the warehouse.
Implements SCD Type 2 logic for historical tracking.
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('change_processor.log')
    ]
)
logger = logging.getLogger(__name__)

class ChangeProcessor:
    """
    Processes CDC change logs and applies SCD Type 2 transformations
    for loading into the warehouse database.
    """
    
    def __init__(self):
        """Initialize database connection to warehouse."""
        self.warehouse_connection = None
        self.cdc_logs_dir = Path("data/cdc_logs")
        self.processed_log = Path("data/cdc_logs/.processed_files")
        self._connect()
        self._ensure_processed_log()
        
    def _ensure_processed_log(self) -> None:
        """Ensure processed files tracking exists."""
        self.processed_log.touch()
        
    def _connect(self) -> None:
        """Connect to warehouse database."""
        try:
            self.warehouse_connection = psycopg2.connect(
                host=os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
                port=os.getenv('WAREHOUSE_DB_PORT', '5433'),
                database=os.getenv('WAREHOUSE_DB_NAME', 'warehouse_db'),
                user=os.getenv('WAREHOUSE_DB_USER', 'postgres'),
                password=os.getenv('WAREHOUSE_DB_PASSWORD', 'postgres')
            )
            self.warehouse_connection.autocommit = False
            logger.info("Successfully connected to warehouse_db")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to warehouse_db: {e}")
            raise
    
    def _create_warehouse_schema(self) -> None:
        """Create SCD Type 2 schema in warehouse."""
        try:
            with self.warehouse_connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS orders_dim (
                        surrogate_key SERIAL PRIMARY KEY,
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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(order_key, valid_from)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_orders_dim_order_key ON orders_dim(order_key);
                    CREATE INDEX IF NOT EXISTS idx_orders_dim_current ON orders_dim(is_current);
                    CREATE INDEX IF NOT EXISTS idx_orders_dim_valid_from ON orders_dim(valid_from);
                """))
                
                self.warehouse_connection.commit()
                logger.info("Created warehouse schema")
                
        except psycopg2.Error as e:
            self.warehouse_connection.rollback()
            logger.error(f"Failed to create warehouse schema: {e}")
            raise
    
    def _get_processed_files(self) -> set:
        """Get set of already processed CDC log files."""
        if not self.processed_log.exists():
            return set()
        
        try:
            with open(self.processed_log, 'r') as f:
                return set(line.strip() for line in f if line.strip())
        except IOError:
            return set()
    
    def _mark_file_processed(self, filename: str) -> None:
        """Mark a CDC log file as processed."""
        try:
            with open(self.processed_log, 'a') as f:
                f.write(f"{filename}\n")
        except IOError as e:
            logger.error(f"Failed to mark file as processed: {e}")
    
    def _process_change_record(self, change: Dict[str, Any]) -> None:
        """
        Process a single change record using SCD Type 2 logic.
        
        Args:
            change: Change record from CDC log
        """
        try:
            with self.warehouse_connection.cursor() as cursor:
                order_key = change['id']
                operation = change['operation_type']
                cdc_timestamp = change['cdc_timestamp']
                
                if operation == 'DELETE':
                    # For deletes, close the current record
                    cursor.execute(sql.SQL("""
                        UPDATE orders_dim 
                        SET valid_to = %s, is_current = FALSE
                        WHERE order_key = %s AND is_current = TRUE
                    """), (cdc_timestamp, order_key))
                    
                elif operation == 'INSERT':
                    # For inserts, create new current record
                    cursor.execute(sql.SQL("""
                        INSERT INTO orders_dim (
                            order_key, customer_id, product_id, quantity,
                            unit_price, total_amount, order_status, order_date,
                            valid_from, cdc_operation, cdc_timestamp
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """), (
                        order_key,
                        change['customer_id'],
                        change['product_id'],
                        change['quantity'],
                        change['unit_price'],
                        change['total_amount'],
                        change['order_status'],
                        change['order_date'],
                        cdc_timestamp,
                        operation,
                        cdc_timestamp
                    ))
                    
                elif operation == 'UPDATE':
                    # For updates, close current record and create new one
                    cursor.execute(sql.SQL("""
                        UPDATE orders_dim 
                        SET valid_to = %s, is_current = FALSE
                        WHERE order_key = %s AND is_current = TRUE
                    """), (cdc_timestamp, order_key))
                    
                    cursor.execute(sql.SQL("""
                        INSERT INTO orders_dim (
                            order_key, customer_id, product_id, quantity,
                            unit_price, total_amount, order_status, order_date,
                            valid_from, cdc_operation, cdc_timestamp
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """), (
                        order_key,
                        change['customer_id'],
                        change['product_id'],
                        change['quantity'],
                        change['unit_price'],
                        change['total_amount'],
                        change['order_status'],
                        change['order_date'],
                        cdc_timestamp,
                        operation,
                        cdc_timestamp
                    ))
                
                self.warehouse_connection.commit()
                logger.debug(f"Processed {operation} for order {order_key}")
                
        except psycopg2.Error as e:
            self.warehouse_connection.rollback()
            logger.error(f"Failed to process change record: {e}")
            raise
    
    def process_cdc_logs(self) -> None:
        """Process all unprocessed CDC log files."""
        logger.info("Starting CDC log processing")
        
        # Ensure warehouse schema exists
        self._create_warehouse_schema()
        
        # Get processed files
        processed_files = self._get_processed_files()
        
        # Find unprocessed batch files
        batch_files = sorted(self.cdc_logs_dir.glob("changes_*.json"))
        
        for batch_file in batch_files:
            if batch_file.name in processed_files:
                continue
            
            logger.info(f"Processing batch file: {batch_file}")
            
            try:
                with open(batch_file, 'r') as f:
                    batch_data = json.load(f)
                
                changes = batch_data.get('changes', [])
                processed_count = 0
                
                for change in changes:
                    try:
                        self._process_change_record(change)
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Failed to process change: {e}")
                        continue
                
                # Mark file as processed
                self._mark_file_processed(batch_file.name)
                logger.info(f"Processed {processed_count}/{len(changes)} changes from {batch_file}")
                
            except Exception as e:
                logger.error(f"Failed to process batch file {batch_file}: {e}")
                continue
        
        logger.info("CDC log processing completed")

def main():
    """Main entry point for change processor."""
    logger.info("Starting Change Data Processor")
    
    try:
        processor = ChangeProcessor()
        processor.process_cdc_logs()
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
