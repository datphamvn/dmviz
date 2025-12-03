"""
Migration Script: Add DateKey to existing Fact_Sales
=====================================================

Script này thêm column DateKey vào Fact_Sales table đã có sẵn,
KHÔNG cần re-run toàn bộ ETL pipeline.

Chạy 1 lần duy nhất để migrate data hiện tại.

Usage:
    python migrations/add_datekey_to_fact_sales.py
    
    hoặc qua Airflow:
    airflow dags trigger migration_add_datekey

Author: Data Engineering Team
Created: December 2025
"""

import os
import logging
import psycopg2
from datetime import datetime

# Configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', "192.168.1.6")
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', "5432")
POSTGRES_DB = os.environ.get('POSTGRES_DB', "retail_dw")
POSTGRES_USER = os.environ.get('POSTGRES_USER', "airflow")
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', "airflow")
POSTGRES_SCHEMA = os.environ.get('POSTGRES_SCHEMA', "public")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def migrate_add_datekey():
    """
    Add DateKey column to existing Fact_Sales table
    and populate it from InvoiceDate.
    
    This is idempotent - safe to run multiple times.
    """
    logger.info("=" * 60)
    logger.info("MIGRATION: Add DateKey to Fact_Sales")
    logger.info("=" * 60)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Step 1: Check if column already exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_schema = %s 
                AND table_name = 'fact_sales' 
                AND column_name = 'datekey'
            )
        """, (POSTGRES_SCHEMA,))
        
        column_exists = cursor.fetchone()[0]
        
        if column_exists:
            # Check if already populated
            cursor.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.fact_sales WHERE datekey IS NULL")
            null_count = cursor.fetchone()[0]
            
            if null_count == 0:
                logger.info("✓ DateKey column already exists and is fully populated. Nothing to do.")
                return {'status': 'already_migrated', 'rows_updated': 0}
            else:
                logger.info(f"DateKey column exists but {null_count:,} rows have NULL values. Updating...")
        else:
            # Step 2: Add column
            logger.info("Adding DateKey column to Fact_Sales...")
            cursor.execute(f"""
                ALTER TABLE {POSTGRES_SCHEMA}.fact_sales 
                ADD COLUMN datekey INT
            """)
            conn.commit()
            logger.info("✓ Column added")
        
        # Step 3: Populate DateKey from InvoiceDate
        logger.info("Populating DateKey from InvoiceDate...")
        cursor.execute(f"""
            UPDATE {POSTGRES_SCHEMA}.fact_sales 
            SET datekey = TO_CHAR(invoicedate, 'YYYYMMDD')::INT
            WHERE datekey IS NULL
        """)
        rows_updated = cursor.rowcount
        conn.commit()
        logger.info(f"✓ Updated {rows_updated:,} rows")
        
        # Step 4: Create index if not exists
        logger.info("Creating index on DateKey...")
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_fact_sales_datekey 
            ON {POSTGRES_SCHEMA}.fact_sales(datekey)
        """)
        conn.commit()
        logger.info("✓ Index created")
        
        # Step 5: Verify
        cursor.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.fact_sales WHERE datekey IS NOT NULL")
        total_with_datekey = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.fact_sales")
        total_rows = cursor.fetchone()[0]
        
        logger.info(f"✓ Verification: {total_with_datekey:,} / {total_rows:,} rows have DateKey")
        
        result = {
            'status': 'success',
            'rows_updated': rows_updated,
            'total_rows': total_rows,
            'migrated_at': datetime.now().isoformat()
        }
        
        logger.info("=" * 60)
        logger.info("MIGRATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Migration failed: {str(e)}")
        raise
        
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    result = migrate_add_datekey()
    print(f"\nResult: {result}")
