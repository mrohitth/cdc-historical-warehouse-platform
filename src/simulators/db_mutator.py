#!/usr/bin/env python3
"""
Database Mutator Simulator for CDC Platform

This script simulates real application traffic by performing periodic
inserts, updates, and deletes records in an orders table in operational_db.
"""

import os
import sys
import time
import random
import signal
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from faker import Faker
from dotenv import load_dotenv

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logging_config import setup_logging
from src.utils.signal_handler import GracefulShutdownHandler, DatabaseConnectionManager

# Load environment variables
load_dotenv()

# Configure structured logging
logger = setup_logging(__name__, log_level=os.getenv('LOG_LEVEL', 'INFO'))

class DatabaseMutator:
    """
    Handles database mutations for CDC simulation.
    Implements idempotent operations with comprehensive error handling.
    """
    
    def __init__(self):
        """Initialize database connection and faker instance."""
        self.connection = None
        self.faker = Faker()
        
        # Initialize graceful shutdown
        self.shutdown_handler = GracefulShutdownHandler(__name__)
        self.conn_manager = DatabaseConnectionManager(self.shutdown_handler, __name__)
        
        self._connect()
        self.shutdown_handler.start_listening()
        
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
                self.connection.autocommit = True
                self.conn_manager.add_connection(self.connection)
                logger.info("Successfully connected to operational_db")
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to database after all retries")
                    raise
    
    def _get_existing_order_ids(self) -> List[int]:
        """Get list of existing order IDs for update/delete operations."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT id FROM orders ORDER BY id")
                return [row['id'] for row in cursor.fetchall()]
        except psycopg2.Error as e:
            logger.error(f"Failed to fetch existing order IDs: {e}")
            return []
    
    def _insert_order(self) -> Optional[int]:
        """
        Insert a new order record.
        Returns the ID of the inserted order or None if failed.
        """
        try:
            with self.connection.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO orders (customer_id, product_id, quantity, unit_price, order_status)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """)
                
                customer_id = random.randint(1, 1000)
                product_id = random.randint(100, 999)
                quantity = random.randint(1, 10)
                unit_price = round(random.uniform(10.0, 500.0), 2)
                status = random.choice(['pending', 'confirmed', 'shipped', 'completed', 'cancelled'])
                
                cursor.execute(insert_query, (
                    customer_id, product_id, quantity, unit_price, status
                ))
                
                order_id = cursor.fetchone()[0]
                logger.info(f"Inserted new order {order_id}: customer={customer_id}, product={product_id}, amount={quantity * unit_price:.2f}")
                return order_id
                
        except psycopg2.Error as e:
            logger.error(f"Failed to insert order: {e}")
            return None
    
    def _update_order(self, order_id: int) -> bool:
        """
        Update an existing order record.
        Returns True if successful, False otherwise.
        """
        try:
            with self.connection.cursor() as cursor:
                # Randomly choose what to update
                update_type = random.choice(['status', 'quantity', 'both'])
                
                if update_type == 'status':
                    new_status = random.choice(['pending', 'confirmed', 'shipped', 'completed', 'cancelled'])
                    update_query = sql.SQL("""
                        UPDATE orders 
                        SET order_status = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """)
                    cursor.execute(update_query, (new_status, order_id))
                    logger.info(f"Updated order {order_id} status to {new_status}")
                    
                elif update_type == 'quantity':
                    new_quantity = random.randint(1, 15)
                    update_query = sql.SQL("""
                        UPDATE orders 
                        SET quantity = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """)
                    cursor.execute(update_query, (new_quantity, order_id))
                    logger.info(f"Updated order {order_id} quantity to {new_quantity}")
                    
                else:  # both
                    new_status = random.choice(['pending', 'confirmed', 'shipped', 'completed', 'cancelled'])
                    new_quantity = random.randint(1, 15)
                    update_query = sql.SQL("""
                        UPDATE orders 
                        SET order_status = %s, quantity = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """)
                    cursor.execute(update_query, (new_status, new_quantity, order_id))
                    logger.info(f"Updated order {order_id}: status={new_status}, quantity={new_quantity}")
                
                return cursor.rowcount > 0
                
        except psycopg2.Error as e:
            logger.error(f"Failed to update order {order_id}: {e}")
            return False
    
    def _delete_order(self, order_id: int) -> bool:
        """
        Delete an order record.
        Returns True if successful, False otherwise.
        """
        try:
            with self.connection.cursor() as cursor:
                delete_query = sql.SQL("DELETE FROM orders WHERE id = %s")
                cursor.execute(delete_query, (order_id,))
                
                if cursor.rowcount > 0:
                    logger.info(f"Deleted order {order_id}")
                    return True
                else:
                    logger.warning(f"Order {order_id} not found for deletion")
                    return False
                    
        except psycopg2.Error as e:
            logger.error(f"Failed to delete order {order_id}: {e}")
            return False
    
    def _get_operation_stats(self) -> Dict[str, Any]:
        """Get current statistics of the orders table."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                stats_query = sql.SQL("""
                    SELECT 
                        COUNT(*) as total_orders,
                        COUNT(CASE WHEN order_status = 'pending' THEN 1 END) as pending_orders,
                        COUNT(CASE WHEN order_status = 'completed' THEN 1 END) as completed_orders,
                        COUNT(CASE WHEN order_status = 'cancelled' THEN 1 END) as cancelled_orders,
                        MAX(order_date) as latest_order,
                        SUM(total_amount) as total_revenue
                    FROM orders
                """)
                cursor.execute(stats_query)
                return dict(cursor.fetchone())
        except psycopg2.Error as e:
            logger.error(f"Failed to get operation stats: {e}")
            return {}
    
    def simulate_mutations(self, interval_seconds: int = 5) -> None:
        """
        Main simulation loop that performs periodic database mutations.
        
        Args:
            interval_seconds: Time between mutation batches
        """
        logger.info("Starting CDC simulation loop")
        logger.info(f"Mutation interval: {interval_seconds} seconds")
        
        try:
            while True:
                logger.info("=== Starting mutation batch ===")
                
                # Get current stats
                stats = self._get_operation_stats()
                if stats:
                    logger.info(f"Current stats: {stats}")
                
                # Get existing orders for potential updates/deletes
                existing_orders = self._get_existing_order_ids()
                
                # Determine operation mix based on current data state
                total_orders = len(existing_orders)
                
                if total_orders == 0:
                    # If no orders, create some initial data
                    logger.info("No existing orders found, creating initial data")
                    for _ in range(5):
                        self._insert_order()
                else:
                    # Normal operation mix
                    operations = []
                    
                    # Always include some inserts (30% chance)
                    if random.random() < 0.3:
                        operations.extend(['insert'] * random.randint(1, 3))
                    
                    # Include updates if we have orders (40% chance)
                    if existing_orders and random.random() < 0.4:
                        update_count = min(random.randint(1, 3), len(existing_orders))
                        orders_to_update = random.sample(existing_orders, update_count)
                        for order_id in orders_to_update:
                            operations.append(('update', order_id))
                    
                    # Include deletes if we have enough orders (20% chance)
                    if len(existing_orders) > 10 and random.random() < 0.2:
                        delete_count = min(random.randint(1, 2), len(existing_orders) // 2)
                        orders_to_delete = random.sample(existing_orders, delete_count)
                        for order_id in orders_to_delete:
                            operations.append(('delete', order_id))
                    
                    # Execute operations
                    for operation in operations:
                        if isinstance(operation, str) and operation == 'insert':
                            self._insert_order()
                        elif isinstance(operation, tuple):
                            op_type, order_id = operation
                            if op_type == 'update':
                                self._update_order(order_id)
                            elif op_type == 'delete':
                                self._delete_order(order_id)
                
                logger.info("=== Mutation batch completed ===")
                
                # Check for shutdown signal
                if self.shutdown_handler.should_shutdown:
                    logger.info("Shutdown signal received, stopping simulation")
                    break
                    
                time.sleep(interval_seconds)
                
        except Exception as e:
            logger.error(f"Unexpected error in simulation loop: {e}")
            raise
        finally:
            logger.info("Database mutator shutting down...")
            self.shutdown_handler.cleanup()

def main():
    """Main entry point for the database mutator."""
    logger.info("Starting Database Mutator for CDC Historical Warehouse Platform")
    
    try:
        mutator = DatabaseMutator()
        
        # Get interval from environment or use default
        interval = int(os.getenv('MUTATION_INTERVAL_SECONDS', '5'))
        
        mutator.simulate_mutations(interval_seconds=interval)
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
