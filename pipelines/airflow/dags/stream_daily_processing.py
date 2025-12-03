"""
DAG: Stream Daily Processing
Xử lý dữ liệu từ raw_zone (ingested từ API hoặc Kafka) và transform vào staging/curated zone
Được trigger bởi kafka_consumer_to_hdfs hoặc stream_daily_batch DAG
"""

from datetime import datetime, timedelta
import json
import os
import logging
import tempfile
from typing import Dict, Any, List

import pandas as pd
import numpy as np
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

# WebHDFS Configuration
WEBHDFS_HOST = os.environ.get('WEBHDFS_HOST', '192.168.1.6')
WEBHDFS_PORT = os.environ.get('WEBHDFS_PORT', '9870')
HDFS_USER = os.environ.get('HDFS_USER', 'anhth')

# HDFS Paths
RAW_ZONE = '/data_lake/raw_zone/internal_streaming'
STAGING_ZONE = '/data_lake/staging_zone/transactions'
CURATED_ZONE = '/data_lake/curated_zone'

# PostgreSQL Configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', '192.168.1.6')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'retail_dw')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
POSTGRES_SCHEMA = os.environ.get('POSTGRES_SCHEMA', 'public')

logger = logging.getLogger(__name__)


# ============== HDFS HELPER FUNCTIONS ==============

def list_hdfs_directory(hdfs_path: str) -> List[str]:
    """List files in HDFS directory"""
    try:
        url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_path}?op=LISTSTATUS&user.name={HDFS_USER}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            files = data.get('FileStatuses', {}).get('FileStatus', [])
            return [f"{hdfs_path}/{f['pathSuffix']}" for f in files]
        return []
    except Exception as e:
        logger.error(f"Error listing HDFS directory: {e}")
        return []


def read_parquet_from_hdfs(hdfs_path: str) -> pd.DataFrame:
    """Read parquet file from HDFS"""
    try:
        url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_path}?op=OPEN&user.name={HDFS_USER}"
        response = requests.get(url, timeout=120, allow_redirects=True)
        
        if response.status_code == 200:
            import io
            return pd.read_parquet(io.BytesIO(response.content))
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading from HDFS: {e}")
        return pd.DataFrame()


def ensure_hdfs_directory(hdfs_dir: str) -> bool:
    """Ensure HDFS directory exists"""
    try:
        check_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_dir}?op=GETFILESTATUS&user.name={HDFS_USER}"
        response = requests.get(check_url, timeout=30)
        
        if response.status_code == 200:
            return True
        
        mkdir_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_dir}?op=MKDIRS&user.name={HDFS_USER}"
        response = requests.put(mkdir_url, timeout=30)
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error ensuring directory: {e}")
        return False


def upload_to_webhdfs(local_file: str, hdfs_path: str) -> bool:
    """Upload file to HDFS"""
    try:
        create_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name={HDFS_USER}"
        response = requests.put(create_url, allow_redirects=False, timeout=30)
        
        if response.status_code == 307:
            datanode_url = response.headers['Location']
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(datanode_url)
            fixed_url = urlunparse(parsed._replace(netloc=f"{WEBHDFS_HOST}:{parsed.port}"))
            
            with open(local_file, 'rb') as f:
                upload_response = requests.put(fixed_url, data=f, timeout=120)
            
            return upload_response.status_code == 201
        return False
    except Exception as e:
        logger.error(f"Error uploading to HDFS: {e}")
        return False


# ============== DATA QUALITY FUNCTIONS ==============

def validate_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate data quality and return metrics
    """
    quality_report = {
        "total_rows": len(df),
        "columns": list(df.columns),
        "null_counts": {},
        "invalid_records": {},
        "data_quality_score": 100.0,
        "issues": []
    }
    
    required_columns = ['Invoice', 'StockCode', 'Quantity', 'InvoiceDate', 'Price', 'CustomerID', 'Country']
    
    # Check for required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        quality_report["issues"].append(f"Missing columns: {missing_cols}")
        quality_report["data_quality_score"] -= 20
    
    # Check null values
    for col in df.columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            quality_report["null_counts"][col] = int(null_count)
    
    # Check CustomerID nulls (important for RFM)
    if 'CustomerID' in df.columns:
        null_customers = df['CustomerID'].isna().sum()
        null_pct = (null_customers / len(df)) * 100 if len(df) > 0 else 0
        if null_pct > 25:
            quality_report["issues"].append(f"High CustomerID null rate: {null_pct:.1f}%")
            quality_report["data_quality_score"] -= 15
    
    # Check for invalid prices
    if 'Price' in df.columns:
        invalid_price = (df['Price'] <= 0).sum()
        if invalid_price > 0:
            quality_report["invalid_records"]["negative_price"] = int(invalid_price)
            quality_report["issues"].append(f"Invalid prices (<=0): {invalid_price}")
    
    # Check for invalid quantities
    if 'Quantity' in df.columns:
        # Note: negative quantity might be returns, so only flag extreme values
        zero_qty = (df['Quantity'] == 0).sum()
        if zero_qty > 0:
            quality_report["invalid_records"]["zero_quantity"] = int(zero_qty)
    
    return quality_report


# ============== DATA CLEANING FUNCTIONS ==============

def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean transaction data:
    1. Remove records with null CustomerID
    2. Handle invalid prices
    3. Standardize date format
    4. Mark returns (Invoice starts with 'C')
    """
    logger.info(f"Cleaning {len(df)} records")
    original_count = len(df)
    
    # Create a copy
    df_clean = df.copy()
    
    # Remove null CustomerID
    df_clean = df_clean.dropna(subset=['CustomerID'])
    logger.info(f"After removing null CustomerID: {len(df_clean)} records")
    
    # Convert CustomerID to int
    df_clean['CustomerID'] = df_clean['CustomerID'].astype(int)
    
    # Remove invalid prices (<=0), but keep for returns analysis separately
    df_clean = df_clean[df_clean['Price'] > 0]
    logger.info(f"After removing invalid prices: {len(df_clean)} records")
    
    # Parse InvoiceDate if it's string
    if df_clean['InvoiceDate'].dtype == 'object':
        df_clean['InvoiceDate'] = pd.to_datetime(df_clean['InvoiceDate'], errors='coerce')
    
    # Remove rows with invalid dates
    df_clean = df_clean.dropna(subset=['InvoiceDate'])
    
    # Add is_return flag
    df_clean['is_return'] = df_clean['Invoice'].astype(str).str.startswith('C')
    
    # Calculate TotalAmount
    df_clean['TotalAmount'] = df_clean['Quantity'] * df_clean['Price']
    
    # Add processing metadata
    df_clean['_processed_at'] = datetime.utcnow()
    df_clean['_source'] = 'streaming_pipeline'
    
    logger.info(f"Cleaning complete: {original_count} -> {len(df_clean)} records ({len(df_clean)/original_count*100:.1f}%)")
    
    return df_clean


# ============== TASK FUNCTIONS ==============

def extract_raw_data(**context) -> Dict[str, Any]:
    """
    Extract data from raw_zone based on execution date
    """
    execution_date = context['execution_date']
    conf = context.get('dag_run').conf or {}
    
    # Determine date to process
    # If triggered with specific date, use that
    if 'process_date' in conf:
        process_date = datetime.strptime(conf['process_date'], '%Y-%m-%d')
    else:
        process_date = execution_date
    
    year = process_date.year
    month = process_date.month
    day = process_date.day
    
    partition_path = f"{RAW_ZONE}/year={year}/month={month:02d}/day={day:02d}"
    logger.info(f"Looking for data in: {partition_path}")
    
    # List files in partition
    files = list_hdfs_directory(partition_path)
    parquet_files = [f for f in files if f.endswith('.parquet')]
    
    if not parquet_files:
        logger.warning(f"No parquet files found in {partition_path}")
        context['ti'].xcom_push(key='raw_data_path', value=None)
        context['ti'].xcom_push(key='record_count', value=0)
        return {"status": "no_data", "files": 0}
    
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    # Read and combine all parquet files
    dfs = []
    for f in parquet_files:
        df = read_parquet_from_hdfs(f)
        if not df.empty:
            dfs.append(df)
    
    if not dfs:
        logger.warning("All parquet files were empty")
        context['ti'].xcom_push(key='raw_data_path', value=None)
        context['ti'].xcom_push(key='record_count', value=0)
        return {"status": "no_data", "files": len(parquet_files)}
    
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined {len(combined_df)} records from {len(dfs)} files")
    
    # Save combined raw data temporarily
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        combined_df.to_parquet(tmp.name, index=False)
        context['ti'].xcom_push(key='raw_data_path', value=tmp.name)
        context['ti'].xcom_push(key='record_count', value=len(combined_df))
    
    return {
        "status": "success",
        "files": len(parquet_files),
        "records": len(combined_df),
        "date": process_date.strftime('%Y-%m-%d')
    }


def validate_quality(**context) -> Dict[str, Any]:
    """
    Validate data quality and decide whether to proceed
    """
    ti = context['ti']
    raw_data_path = ti.xcom_pull(task_ids='extract_raw_data', key='raw_data_path')
    
    if not raw_data_path:
        logger.warning("No raw data to validate")
        return {"status": "skip", "reason": "no_data"}
    
    df = pd.read_parquet(raw_data_path)
    quality_report = validate_data_quality(df)
    
    logger.info(f"Data Quality Report:")
    logger.info(f"  Total rows: {quality_report['total_rows']}")
    logger.info(f"  Quality score: {quality_report['data_quality_score']}")
    logger.info(f"  Issues: {quality_report['issues']}")
    
    ti.xcom_push(key='quality_report', value=quality_report)
    
    # Decide if quality is acceptable (score > 60)
    if quality_report['data_quality_score'] < 60:
        logger.error(f"Data quality too low: {quality_report['data_quality_score']}")
        return {"status": "failed", "report": quality_report}
    
    return {"status": "passed", "report": quality_report}


def transform_and_clean(**context) -> Dict[str, Any]:
    """
    Clean and transform data, save to staging zone
    """
    ti = context['ti']
    execution_date = context['execution_date']
    raw_data_path = ti.xcom_pull(task_ids='extract_raw_data', key='raw_data_path')
    
    if not raw_data_path:
        return {"status": "skip", "reason": "no_data"}
    
    df = pd.read_parquet(raw_data_path)
    
    # Clean data
    df_clean = clean_transactions(df)
    
    if df_clean.empty:
        logger.warning("All records filtered out during cleaning")
        return {"status": "skip", "reason": "all_filtered"}
    
    # Save to staging zone
    date_str = execution_date.strftime('%Y%m%d')
    staging_path = f"{STAGING_ZONE}/cleaned_transactions_{date_str}.parquet"
    
    ensure_hdfs_directory(STAGING_ZONE)
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        df_clean.to_parquet(tmp.name, index=False)
        
        if upload_to_webhdfs(tmp.name, staging_path):
            logger.info(f"Saved {len(df_clean)} cleaned records to {staging_path}")
            ti.xcom_push(key='staging_path', value=staging_path)
            ti.xcom_push(key='cleaned_count', value=len(df_clean))
            
            # Cleanup temp file
            os.unlink(tmp.name)
            
            return {
                "status": "success",
                "original_count": len(df),
                "cleaned_count": len(df_clean),
                "staging_path": staging_path
            }
        else:
            return {"status": "failed", "reason": "hdfs_upload_failed"}


def compute_daily_aggregates(**context) -> Dict[str, Any]:
    """
    Compute daily aggregates for quick reporting
    """
    ti = context['ti']
    execution_date = context['execution_date']
    staging_path = ti.xcom_pull(task_ids='transform_and_clean', key='staging_path')
    
    if not staging_path:
        return {"status": "skip"}
    
    df = read_parquet_from_hdfs(staging_path)
    
    if df.empty:
        return {"status": "skip", "reason": "empty_data"}
    
    # Filter out returns for aggregates
    df_sales = df[~df['is_return']]
    
    # Daily aggregates
    daily_stats = {
        "date": execution_date.strftime('%Y-%m-%d'),
        "total_revenue": float(df_sales['TotalAmount'].sum()),
        "total_orders": int(df_sales['Invoice'].nunique()),
        "total_customers": int(df_sales['CustomerID'].nunique()),
        "total_items_sold": int(df_sales['Quantity'].sum()),
        "avg_order_value": float(df_sales.groupby('Invoice')['TotalAmount'].sum().mean()),
        "unique_products": int(df_sales['StockCode'].nunique()),
        "top_countries": df_sales['Country'].value_counts().head(5).to_dict(),
        "returns_count": int(df['is_return'].sum()),
        "returns_amount": float(df[df['is_return']]['TotalAmount'].abs().sum())
    }
    
    logger.info(f"Daily Stats for {daily_stats['date']}:")
    logger.info(f"  Revenue: ${daily_stats['total_revenue']:,.2f}")
    logger.info(f"  Orders: {daily_stats['total_orders']}")
    logger.info(f"  Customers: {daily_stats['total_customers']}")
    logger.info(f"  AOV: ${daily_stats['avg_order_value']:,.2f}")
    
    ti.xcom_push(key='daily_stats', value=daily_stats)
    
    # Save aggregates to curated zone
    agg_path = f"{CURATED_ZONE}/daily_stats"
    ensure_hdfs_directory(agg_path)
    
    agg_df = pd.DataFrame([daily_stats])
    date_str = execution_date.strftime('%Y%m%d')
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        agg_df.to_parquet(tmp.name, index=False)
        hdfs_path = f"{agg_path}/daily_stats_{date_str}.parquet"
        upload_to_webhdfs(tmp.name, hdfs_path)
        os.unlink(tmp.name)
    
    return {"status": "success", "stats": daily_stats}


def check_data_exists(**context) -> str:
    """Branch operator to check if we have data to process"""
    ti = context['ti']
    record_count = ti.xcom_pull(task_ids='extract_raw_data', key='record_count')
    
    if record_count and record_count > 0:
        return 'validate_quality'
    return 'no_data_to_process'


# ============== DAG DEFINITION ==============

default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='stream_daily_processing',
    default_args=default_args,
    description='Process streaming data from raw zone: validate, clean, transform, aggregate',
    schedule_interval=None,  # Triggered by other DAGs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=['processing', 'etl', 'streaming'],
) as dag:
    
    # Extract raw data
    extract = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_raw_data,
        provide_context=True,
    )
    
    # Branch based on data availability
    check_data = BranchPythonOperator(
        task_id='check_data_exists',
        python_callable=check_data_exists,
        provide_context=True,
    )
    
    # No data path
    no_data = EmptyOperator(
        task_id='no_data_to_process',
    )
    
    # Validate data quality
    validate = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_quality,
        provide_context=True,
    )
    
    # Transform and clean
    transform = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_and_clean,
        provide_context=True,
    )
    
    # Compute daily aggregates
    aggregate = PythonOperator(
        task_id='compute_daily_aggregates',
        python_callable=compute_daily_aggregates,
        provide_context=True,
    )
    
    # End tasks
    end_success = EmptyOperator(
        task_id='end_success',
        trigger_rule='none_failed_min_one_success',
    )
    
    end_no_data = EmptyOperator(
        task_id='end_no_data',
    )
    
    # Dependencies
    extract >> check_data
    check_data >> [validate, no_data]
    validate >> transform >> aggregate >> end_success
    no_data >> end_no_data
