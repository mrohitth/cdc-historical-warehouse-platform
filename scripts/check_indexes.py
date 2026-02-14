#!/usr/bin/env python3
"""
Check warehouse database indexes for dim_orders_history table.
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_indexes():
    conn = psycopg2.connect(
        host=os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
        port=os.getenv('WAREHOUSE_DB_PORT', '5433'),
        database=os.getenv('WAREHOUSE_DB_NAME', 'warehouse_db'),
        user=os.getenv('WAREHOUSE_DB_USER', 'postgres'),
        password=os.getenv('WAREHOUSE_DB_PASSWORD', 'postgres')
    )

    cursor = conn.cursor()
    cursor.execute("""
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'dim_orders_history'
ORDER BY indexname;
""")

    indexes = cursor.fetchall()
    print('Current indexes on dim_orders_history:')
    for idx in indexes:
        print(f'  {idx[0]}: {idx[1]}')

    conn.close()

if __name__ == "__main__":
    check_indexes()
