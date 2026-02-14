#!/usr/bin/env python3
"""
Test rapid updates scenario for SCD Type 2 loader.
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

import psycopg2
import json
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

def create_test_batch():
    """Create a test batch with multiple updates to the same order."""
    
    # Create test changes for the same order with different timestamps
    base_time = datetime.now(timezone.utc)
    
    changes = [
        {
            "id": 999,  # test order ID
            "customer_id": 100,
            "product_id": 200,
            "quantity": 1,
            "unit_price": 10.00,
            "total_amount": 10.00,
            "order_status": "pending",
            "order_date": "2026-02-01T20:00:00",
            "last_updated": "2026-02-01T20:00:00",
            "created_at": "2026-02-01T20:00:00",
            "operation_type": "INSERT",
            "cdc_timestamp": (base_time).isoformat(),
            "extracted_at": (base_time).isoformat()
        },
        {
            "id": 999,
            "customer_id": 100,
            "product_id": 200,
            "quantity": 2,
            "unit_price": 10.00,
            "total_amount": 20.00,
            "order_status": "confirmed",
            "order_date": "2026-02-01T20:00:00",
            "last_updated": "2026-02-01T20:01:00",
            "created_at": "2026-02-01T20:00:00",
            "operation_type": "UPDATE",
            "cdc_timestamp": (base_time.replace(second=1)).isoformat(),
            "extracted_at": (base_time.replace(second=1)).isoformat()
        },
        {
            "id": 999,
            "customer_id": 100,
            "product_id": 200,
            "quantity": 3,
            "unit_price": 15.00,
            "total_amount": 45.00,
            "order_status": "shipped",
            "order_date": "2026-02-01T20:00:00",
            "last_updated": "2026-02-01T20:02:00",
            "created_at": "2026-02-01T20:00:00",
            "operation_type": "UPDATE",
            "cdc_timestamp": (base_time.replace(second=2)).isoformat(),
            "extracted_at": (base_time.replace(second=2)).isoformat()
        }
    ]
    
    # Create batch data
    batch_data = {
        "batch_metadata": {
            "extracted_at": base_time.isoformat(),
            "change_count": len(changes),
            "watermark": (base_time - timedelta(minutes=5)).isoformat()
        },
        "changes": changes
    }
    
    # Ensure data/cdc_logs directory exists
    cdc_dir = Path("data/cdc_logs")
    cdc_dir.mkdir(parents=True, exist_ok=True)
    
    # Write batch file
    timestamp = base_time.strftime("%Y%m%d_%H%M%S_%f")[:-3]
    batch_file = cdc_dir / f"test_rapid_updates_{timestamp}.json"
    
    with open(batch_file, 'w') as f:
        json.dump(batch_data, f, indent=2)
    
    print(f"Created test batch file: {batch_file}")
    return batch_file

def test_rapid_updates():
    """Test the rapid updates handling."""
    print("Testing rapid updates scenario...")
    
    # Create test batch
    batch_file = create_test_batch()
    
    # Run SCD Type 2 loader
    print("Running SCD Type 2 loader...")
    result = os.system(f"python3 src/warehouse/scd2_loader.py")
    
    if result == 0:
        print("✅ SCD Type 2 loader completed successfully")
    else:
        print("❌ SCD Type 2 loader failed")
        return False
    
    # Check results in warehouse
    print("\nChecking results in warehouse...")
    conn = psycopg2.connect(
        host=os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
        port=os.getenv('WAREHOUSE_DB_PORT', '5433'),
        database=os.getenv('WAREHOUSE_DB_NAME', 'warehouse_db'),
        user=os.getenv('WAREHOUSE_DB_USER', 'postgres'),
        password=os.getenv('WAREHOUSE_DB_PASSWORD', 'postgres')
    )
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT surrogate_key, order_key, quantity, unit_price, order_status,
               valid_from, valid_to, is_current, cdc_operation
        FROM dim_orders_history 
        WHERE order_key = 999
        ORDER BY valid_from
    """)
    
    records = cursor.fetchall()
    
    print(f"\nFound {len(records)} records for order 999:")
    for record in records:
        print(f"  Key: {record[0]}, Qty: {record[2]}, Price: {record[3]}, "
              f"Status: {record[4]}, Current: {record[7]}, Op: {record[8]}")
    
    # Validate rapid updates handling
    current_records = [r for r in records if r[7]]  # is_current
    historical_records = [r for r in records if not r[7]]
    
    print(f"\nValidation:")
    print(f"  Current records: {len(current_records)}")
    print(f"  Historical records: {len(historical_records)}")
    
    # Should have exactly 2 records: 1 historical (INSERT) + 1 current (latest UPDATE)
    if len(current_records) == 1 and len(historical_records) == 1:
        print("✅ Rapid updates handled correctly - only latest version is current")
        
        # Check that current record has the latest values
        current = current_records[0]
        if (current[2] == 3 and current[3] == 15.00 and current[4] == "shipped"):
            print("✅ Current record has correct latest values")
            return True
        else:
            print("❌ Current record has incorrect values")
            return False
    else:
        print("❌ Incorrect number of records after rapid updates")
        return False

if __name__ == "__main__":
    success = test_rapid_updates()
    sys.exit(0 if success else 1)
