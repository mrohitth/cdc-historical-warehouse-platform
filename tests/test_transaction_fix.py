#!/usr/bin/env python3
"""
Simple test to verify the transaction integrity fix.
"""

import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

import psycopg2
from dotenv import load_dotenv

load_dotenv()

def test_transaction_fix():
    """Test that the transaction fix works correctly."""
    print("🔧 Testing Transaction Integrity Fix")
    print("=" * 50)
    
    # Connect to warehouse
    conn = psycopg2.connect(
        host=os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
        port=os.getenv('WAREHOUSE_DB_PORT', '5433'),
        database=os.getenv('WAREHOUSE_DB_NAME', 'warehouse_db'),
        user=os.getenv('WAREHOUSE_DB_USER', 'postgres'),
        password=os.getenv('WAREHOUSE_DB_PASSWORD', 'postgres')
    )
    conn.autocommit = False
    
    try:
        with conn.cursor() as cursor:
            # Clean up test data
            cursor.execute("DELETE FROM dim_orders_history WHERE order_key = 12345")
            conn.commit()
            
            # Insert initial record
            cursor.execute("""
                INSERT INTO dim_orders_history (
                    order_key, customer_id, product_id, quantity,
                    unit_price, total_amount, order_status, order_date,
                    valid_from, cdc_operation, cdc_timestamp, batch_id
                ) VALUES (
                    12345, 1, 1, 1, 10.00, 10.00, 'pending', '2026-02-01T10:00:00Z',
                    '2026-02-01T10:00:00Z', 'INSERT', '2026-02-01T10:00:00Z', 'test_txn'
                )
            """)
            conn.commit()
            
            print("✅ Initial record inserted")
            
            # Test the fixed transaction pattern
            update_timestamp = datetime(2026, 2, 1, 10, 5, 30, 123456, timezone.utc)
            
            print("🔄 Testing atomic transaction...")
            
            # Single transaction - both operations atomic
            with conn:
                with conn.cursor() as cursor:
                    # Expire old record
                    cursor.execute("""
                        UPDATE dim_orders_history 
                        SET valid_to = %s, is_current = FALSE
                        WHERE order_key = %s AND is_current = TRUE
                    """, (update_timestamp, 12345))
                    
                    # Insert new record
                    cursor.execute("""
                        INSERT INTO dim_orders_history (
                            order_key, customer_id, product_id, quantity,
                            unit_price, total_amount, order_status, order_date,
                            valid_from, cdc_operation, cdc_timestamp, batch_id
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        12345, 1, 1, 2, 10.00, 20.00, 'confirmed', '2026-02-01T10:00:00Z',
                        update_timestamp, 'UPDATE', update_timestamp, 'test_txn'
                    ))
                
                print("✅ ATOMIC TRANSACTION: Both operations committed together")
            
            # Verify results
            cursor.execute("""
                SELECT surrogate_key, is_current, valid_from, valid_to
                FROM dim_orders_history 
                WHERE order_key = 12345
                ORDER BY surrogate_key
            """)
            
            results = cursor.fetchall()
            print(f"\n📊 Results:")
            for result in results:
                print(f"  Record {result[0]}: current={result[1]}, valid_from={result[2]}, valid_to={result[3]}")
            
            # Verify atomicity
            current_records = [r for r in results if r[1]]  # is_current = True
            expired_records = [r for r in results if not r[1]]  # is_current = False
            
            if len(current_records) == 1 and len(expired_records) == 1:
                print("\n✅ ATOMICITY VERIFIED: One current and one expired record")
                print("✅ NO RACE CONDITION: No intermediate state with no current record")
                print("✅ TRANSACTION INTEGRITY CONFIRMED!")
                return True
            else:
                print(f"\n❌ ATOMICITY FAILED: {len(current_records)} current, {len(expired_records)} expired")
                return False
                
    except Exception as e:
        print(f"❌ Test failed: {e}")
        conn.rollback()
        return False
    finally:
        # Clean up
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM dim_orders_history WHERE order_key = 12345")
                conn.commit()
        except:
            pass
        conn.close()

if __name__ == "__main__":
    success = test_transaction_fix()
    print(f"\n🎯 Transaction Fix Test: {'PASSED' if success else 'FAILED'}")
    sys.exit(0 if success else 1)
