"""
DAG: Refresh Materialized Views
================================

Pipeline để refresh các Materialized Views trong Data Warehouse
sau khi dữ liệu mới được load.

Schedule: Chạy sau khi historical_data_etl_processing hoàn thành
          hoặc sau khi extract_consumer_trends load macro data mới

Author: Data Engineering Team
Created: December 2025
"""

from datetime import datetime, timedelta
import os
import logging
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', "192.168.1.6")
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', "5432")
POSTGRES_DB = os.environ.get('POSTGRES_DB', "retail_dw")
POSTGRES_USER = os.environ.get('POSTGRES_USER', "airflow")
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', "airflow")
POSTGRES_SCHEMA = os.environ.get('POSTGRES_SCHEMA', "public")

logger = logging.getLogger(__name__)


def get_postgres_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def task_check_prerequisites(**context):
    """
    Check that prerequisite tables exist before refreshing views
    """
    logger.info("=" * 60)
    logger.info("TASK: Checking Prerequisites")
    logger.info("=" * 60)
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    required_tables = [
        'fact_sales',
        'dim_customer', 
        'dim_product',
        'dim_date',
        'dim_macro_economic'
    ]
    
    missing_tables = []
    table_counts = {}
    
    try:
        for table in required_tables:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{POSTGRES_SCHEMA}' 
                    AND table_name = '{table}'
                )
            """)
            exists = cursor.fetchone()[0]
            
            if exists:
                cursor.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.{table}")
                count = cursor.fetchone()[0]
                table_counts[table] = count
                logger.info(f"✓ {table}: {count:,} records")
            else:
                missing_tables.append(table)
                logger.warning(f"✗ {table}: NOT FOUND")
        
        if missing_tables:
            logger.warning(f"Missing tables: {missing_tables}")
            logger.warning("Some materialized views may not be created")
        
        return {
            'table_counts': table_counts,
            'missing_tables': missing_tables,
            'all_present': len(missing_tables) == 0
        }
        
    finally:
        cursor.close()
        conn.close()


def task_create_mv_customer_quarterly_analysis(**context):
    """
    Create/Refresh the Customer Quarterly Analysis Materialized View
    
    This view pre-aggregates customer spending per quarter with macro context
    """
    logger.info("=" * 60)
    logger.info("TASK: Creating mv_customer_quarterly_analysis")
    logger.info("=" * 60)
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Check if materialized view exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_matviews 
                WHERE schemaname = %s 
                AND matviewname = 'mv_customer_quarterly_analysis'
            )
        """, (POSTGRES_SCHEMA,))
        mv_exists = cursor.fetchone()[0]
        
        if mv_exists:
            # Refresh existing materialized view
            logger.info("Refreshing existing materialized view...")
            cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {POSTGRES_SCHEMA}.mv_customer_quarterly_analysis")
            logger.info("✓ Materialized view refreshed")
        else:
            # Create new materialized view
            logger.info("Creating new materialized view...")
            
            create_sql = f"""
            CREATE MATERIALIZED VIEW {POSTGRES_SCHEMA}.mv_customer_quarterly_analysis AS
            SELECT 
                -- Customer Identifiers
                dc.customerkey,
                dc.customerid,
                dc.rfm_segment,
                
                -- Time Dimension
                dd.year,
                dd.quarter,
                dd.quarterkey,
                
                -- Customer Quarterly Metrics
                COUNT(DISTINCT fs.invoice) AS quarterly_orders,
                SUM(fs.quantity) AS quarterly_items,
                SUM(fs.totalamount) AS quarterly_spend,
                AVG(fs.totalamount) AS avg_order_value,
                
                -- Macro Economic Context
                me.hfc_total_cvm_sa AS national_hfc_total,
                me.hfc_total_growth_qoq AS national_hfc_growth,
                me.coicop_02_alcohol_tobacco AS national_alcohol_spend,
                me.coicop_11_restaurants AS national_restaurants_spend,
                
                -- Calculated Metrics
                CASE 
                    WHEN me.hfc_total_cvm_sa > 0 
                    THEN SUM(fs.totalamount) / me.hfc_total_cvm_sa * 100
                    ELSE 0 
                END AS spend_vs_national_pct
                
            FROM {POSTGRES_SCHEMA}.fact_sales fs
            JOIN {POSTGRES_SCHEMA}.dim_customer dc ON fs.customerkey = dc.customerkey
            JOIN {POSTGRES_SCHEMA}.dim_date dd ON fs.datekey = dd.datekey
            LEFT JOIN {POSTGRES_SCHEMA}.dim_macro_economic me ON dd.quarterkey = me.quarterkey
            GROUP BY 
                dc.customerkey,
                dc.customerid,
                dc.rfm_segment,
                dd.year,
                dd.quarter,
                dd.quarterkey,
                me.hfc_total_cvm_sa,
                me.hfc_total_growth_qoq,
                me.coicop_02_alcohol_tobacco,
                me.coicop_11_restaurants;
            """
            cursor.execute(create_sql)
            
            # Create unique index for concurrent refresh
            cursor.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cqa_customer_quarter 
                ON {POSTGRES_SCHEMA}.mv_customer_quarterly_analysis(customerkey, quarterkey)
            """)
            
            # Create additional indexes
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_mv_cqa_segment 
                ON {POSTGRES_SCHEMA}.mv_customer_quarterly_analysis(rfm_segment)
            """)
            
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_mv_cqa_quarter 
                ON {POSTGRES_SCHEMA}.mv_customer_quarterly_analysis(quarterkey)
            """)
            
            logger.info("✓ Materialized view created with indexes")
        
        conn.commit()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.mv_customer_quarterly_analysis")
        count = cursor.fetchone()[0]
        
        return {
            'view_name': 'mv_customer_quarterly_analysis',
            'records': count,
            'status': 'refreshed' if mv_exists else 'created'
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating/refreshing MV: {str(e)}")
        raise
        
    finally:
        cursor.close()
        conn.close()


def task_create_mv_segment_economic_performance(**context):
    """
    Create/Refresh Segment Economic Performance Materialized View
    
    Analyzes RFM segment performance during growth vs contraction periods
    """
    logger.info("=" * 60)
    logger.info("TASK: Creating mv_segment_economic_performance")
    logger.info("=" * 60)
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Check if materialized view exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM pg_matviews 
                WHERE schemaname = %s 
                AND matviewname = 'mv_segment_economic_performance'
            )
        """, (POSTGRES_SCHEMA,))
        mv_exists = cursor.fetchone()[0]
        
        if mv_exists:
            logger.info("Refreshing existing materialized view...")
            cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {POSTGRES_SCHEMA}.mv_segment_economic_performance")
            logger.info("✓ Materialized view refreshed")
        else:
            logger.info("Creating new materialized view...")
            
            create_sql = f"""
            CREATE MATERIALIZED VIEW {POSTGRES_SCHEMA}.mv_segment_economic_performance AS
            SELECT 
                dc.rfm_segment,
                CASE 
                    WHEN me.hfc_total_growth_qoq >= 0 THEN 'Growth Period'
                    ELSE 'Contraction Period'
                END AS economic_period,
                COUNT(DISTINCT dc.customerid) AS customer_count,
                SUM(fs.totalamount) AS total_revenue,
                AVG(fs.totalamount) AS avg_transaction,
                COUNT(fs.saleskey) AS transaction_count,
                SUM(fs.totalamount) / NULLIF(COUNT(DISTINCT dc.customerid), 0) AS revenue_per_customer
            FROM {POSTGRES_SCHEMA}.fact_sales fs
            JOIN {POSTGRES_SCHEMA}.dim_customer dc ON fs.customerkey = dc.customerkey
            JOIN {POSTGRES_SCHEMA}.dim_date dd ON fs.datekey = dd.datekey
            LEFT JOIN {POSTGRES_SCHEMA}.dim_macro_economic me ON dd.quarterkey = me.quarterkey
            WHERE me.hfc_total_growth_qoq IS NOT NULL
            GROUP BY 
                dc.rfm_segment, 
                CASE 
                    WHEN me.hfc_total_growth_qoq >= 0 THEN 'Growth Period'
                    ELSE 'Contraction Period'
                END;
            """
            cursor.execute(create_sql)
            
            # Create unique index
            cursor.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sep_segment_period 
                ON {POSTGRES_SCHEMA}.mv_segment_economic_performance(rfm_segment, economic_period)
            """)
            
            logger.info("✓ Materialized view created")
        
        conn.commit()
        
        cursor.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.mv_segment_economic_performance")
        count = cursor.fetchone()[0]
        
        return {
            'view_name': 'mv_segment_economic_performance',
            'records': count,
            'status': 'refreshed' if mv_exists else 'created'
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error: {str(e)}")
        raise
        
    finally:
        cursor.close()
        conn.close()


def task_validate_materialized_views(**context):
    """
    Validate all materialized views are up to date
    """
    logger.info("=" * 60)
    logger.info("TASK: Validating Materialized Views")
    logger.info("=" * 60)
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    validation_results = {}
    
    try:
        # List all materialized views
        cursor.execute("""
            SELECT matviewname, definition
            FROM pg_matviews 
            WHERE schemaname = %s
        """, (POSTGRES_SCHEMA,))
        
        mvs = cursor.fetchall()
        logger.info(f"Found {len(mvs)} materialized views")
        
        for mv_name, _ in mvs:
            cursor.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.{mv_name}")
            count = cursor.fetchone()[0]
            validation_results[mv_name] = count
            logger.info(f"  ✓ {mv_name}: {count:,} records")
        
        return {
            'materialized_views': validation_results,
            'total_views': len(mvs)
        }
        
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='refresh_materialized_views',
    default_args=default_args,
    description='Refresh Data Warehouse Materialized Views',
    schedule_interval=None,  # Manual trigger or triggered after ETL
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['maintenance', 'materialized-view', 'postgresql', 'data-warehouse'],
    doc_md=__doc__,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    check_prerequisites = PythonOperator(
        task_id='check_prerequisites',
        python_callable=task_check_prerequisites,
    )
    
    create_mv_customer_quarterly = PythonOperator(
        task_id='create_mv_customer_quarterly_analysis',
        python_callable=task_create_mv_customer_quarterly_analysis,
    )
    
    create_mv_segment_perf = PythonOperator(
        task_id='create_mv_segment_economic_performance',
        python_callable=task_create_mv_segment_economic_performance,
    )
    
    validate_mvs = PythonOperator(
        task_id='validate_materialized_views',
        python_callable=task_validate_materialized_views,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencies
    start >> check_prerequisites
    check_prerequisites >> [create_mv_customer_quarterly, create_mv_segment_perf]
    [create_mv_customer_quarterly, create_mv_segment_perf] >> validate_mvs >> end
