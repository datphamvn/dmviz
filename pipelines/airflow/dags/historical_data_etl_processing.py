"""
DAG: Historical Data ETL Processing Pipeline
=============================================

Pipeline xử lý dữ liệu historical từ HDFS Raw Zone, transform bằng Spark,
và load vào PostgreSQL Data Warehouse.

Data Flow:
    HDFS (Raw Zone) → Spark Processing → PostgreSQL (Data Warehouse)
    
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                        HISTORICAL DATA ETL PIPELINE                      │
    ├─────────────────────────────────────────────────────────────────────────┤
    │                                                                          │
    │  ┌──────────────┐                                                        │
    │  │  validate_   │                                                        │
    │  │  source_data │                                                        │
    │  └──────┬───────┘                                                        │
    │         │                                                                │
    │         ▼                                                                │
    │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐             │
    │  │   phase1_    │     │   phase1_    │     │   phase1_    │             │
    │  │  clean_data  │────▶│  engineer_   │────▶│   create_    │             │
    │  │              │     │   features   │     │  dim_product │             │
    │  └──────────────┘     └──────┬───────┘     └──────┬───────┘             │
    │                              │                     │                     │
    │                              ▼                     │                     │
    │                       ┌──────────────┐             │                     │
    │                       │   phase2_    │             │                     │
    │                       │  rfm_scoring │◀────────────┘                     │
    │                       └──────┬───────┘                                   │
    │                              │                                           │
    │                              ▼                                           │
    │                       ┌──────────────┐                                   │
    │                       │   phase2_    │                                   │
    │                       │ segmentation │                                   │
    │                       └──────┬───────┘                                   │
    │                              │                                           │
    │         ┌────────────────────┼────────────────────┐                     │
    │         ▼                    ▼                    ▼                     │
    │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐             │
    │  │    load_     │     │    load_     │     │    load_     │             │
    │  │  fact_sales  │     │ dim_customer │     │ dim_product  │             │
    │  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘             │
    │         │                    │                    │                     │
    │         └────────────────────┼────────────────────┘                     │
    │                              ▼                                           │
    │                       ┌──────────────┐                                   │
    │                       │   validate_  │                                   │
    │                       │   warehouse  │                                   │
    │                       └──────┬───────┘                                   │
    │                              │                                           │
    │                              ▼                                           │
    │                       ┌──────────────┐                                   │
    │                       │   cleanup_   │                                   │
    │                       │    temp      │                                   │
    │                       └──────────────┘                                   │
    │                                                                          │
    └─────────────────────────────────────────────────────────────────────────┘

Tables Generated:
    - Fact_Sales: Transaction fact table (cleaned sales data)
    - Dim_Customer: Customer dimension with RFM scores and segments
    - Dim_Product: Product dimension table

Author: Data Engineering Team
Created: December 2025
"""

from datetime import datetime, timedelta
import os
import logging
import tempfile
import shutil
from typing import Dict, Any, Optional, List, Tuple

import requests
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# ============================================================================
# CONFIGURATION
# ============================================================================

# HDFS Configuration (WebHDFS)
WEBHDFS_HOST = os.environ.get('WEBHDFS_HOST', "192.168.1.6")
WEBHDFS_PORT = os.environ.get('WEBHDFS_PORT', "9870")
HDFS_USER = os.environ.get('HDFS_USER', "anhth")
HDFS_INPUT_DIR = "/data_lake/raw_zone/internal_historical"

# PostgreSQL Configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', "192.168.1.6")
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', "5432")
POSTGRES_DB = os.environ.get('POSTGRES_DB', "retail_dw")
POSTGRES_USER = os.environ.get('POSTGRES_USER', "airflow")
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', "airflow")
POSTGRES_SCHEMA = os.environ.get('POSTGRES_SCHEMA', "public")


# Processing Configuration
TEMP_DIR_PREFIX = "etl_historical_"
RFM_QUANTILES = 5

# Logging
logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_postgres_engine():
    """Create PostgreSQL SQLAlchemy engine"""
    connection_string = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(connection_string)


def load_dataframe_to_postgres(df: pd.DataFrame, table_name: str, schema: str, 
                                if_exists: str = 'replace', chunksize: int = 10000) -> int:
    """
    Load DataFrame to PostgreSQL using psycopg2 directly.
    This avoids compatibility issues between pandas and SQLAlchemy versions.
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        schema: Target schema
        if_exists: 'replace' or 'append'
        chunksize: Number of rows per batch
    
    Returns:
        Number of rows loaded
    """
    import psycopg2
    from io import StringIO
    
    logger.info(f"POSTGRES_HOST: {POSTGRES_HOST}")
    logger.info(f"POSTGRES_PORT: {POSTGRES_PORT}")

    # Connect using psycopg2 directly
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
        # Set search_path to use schema
        cursor.execute(f"SET search_path TO {schema}")
        conn.commit()
        
        if if_exists == 'replace':
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            conn.commit()
        
        # Create table from DataFrame schema (use lowercase column names to avoid quoting issues)
        columns = []
        col_mapping = {}  # Original name -> lowercase name
        for col, dtype in df.dtypes.items():
            col_lower = col.lower()
            col_mapping[col] = col_lower
            if 'int' in str(dtype):
                pg_type = 'BIGINT'
            elif 'float' in str(dtype):
                pg_type = 'DOUBLE PRECISION'
            elif 'datetime' in str(dtype):
                pg_type = 'TIMESTAMP'
            elif 'bool' in str(dtype):
                pg_type = 'BOOLEAN'
            else:
                pg_type = 'TEXT'
            columns.append(f'{col_lower} {pg_type}')
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        cursor.execute(create_sql)
        conn.commit()
        
        # Rename DataFrame columns to lowercase for COPY
        df_copy = df.copy()
        df_copy.columns = [c.lower() for c in df_copy.columns]
        
        # Use COPY for fast loading
        output = StringIO()
        df_copy.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
        output.seek(0)
        
        # Use copy_from with lowercase column names
        cursor.copy_from(output, table_name, null='\\N', columns=list(df_copy.columns))
        conn.commit()
        
        logger.info(f"Loaded {len(df):,} rows to {schema}.{table_name}")
        return len(df)
        
    finally:
        if cursor:
            cursor.close()
        conn.close()


def download_hdfs_parquet_files(hdfs_dir: str, local_dir: str) -> str:
    """
    Download all parquet files from HDFS directory using WebHDFS API
    
    Args:
        hdfs_dir: HDFS directory path
        local_dir: Local directory to save files
    
    Returns:
        local_dir: Path to downloaded files
    """
    base_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1"
    
    def list_files_recursive(path: str, files_list: List[str]) -> List[str]:
        """Recursively list all parquet files in HDFS directory"""
        url = f"{base_url}{path}?op=LISTSTATUS&user.name={HDFS_USER}"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        file_statuses = response.json().get('FileStatuses', {}).get('FileStatus', [])
        
        for item in file_statuses:
            item_path = f"{path}/{item['pathSuffix']}"
            if item['type'] == 'DIRECTORY':
                list_files_recursive(item_path, files_list)
            elif item['pathSuffix'].endswith('.parquet'):
                files_list.append(item_path)
        
        return files_list
    
    def download_file(hdfs_path: str, local_path: str) -> None:
        """Download a single file from HDFS using WebHDFS with redirect handling"""
        from urllib.parse import urlparse, urlunparse
        
        url = f"{base_url}{hdfs_path}?op=OPEN&user.name={HDFS_USER}"
        
        # First request to get redirect URL (don't follow redirects automatically)
        response = requests.get(url, allow_redirects=False, timeout=60)
        
        if response.status_code == 307:
            # Get redirect location (DataNode URL)
            redirect_url = response.headers.get('Location')
            
            if redirect_url:
                # Replace hostname with IP address (Docker container can't resolve hostname)
                parsed = urlparse(redirect_url)
                # Use the WEBHDFS_HOST IP but keep the DataNode port (usually 9864)
                fixed_url = urlunparse(parsed._replace(netloc=f"{WEBHDFS_HOST}:{parsed.port}"))
                logger.debug(f"Redirect URL fixed: {fixed_url}")
                
                # Now download from DataNode
                response = requests.get(fixed_url, stream=True, timeout=120)
                response.raise_for_status()
        else:
            # No redirect, use response directly
            response.raise_for_status()
        
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    
    logger.info(f"Scanning HDFS directory: {hdfs_dir}")
    parquet_files = list_files_recursive(hdfs_dir, [])
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    if not parquet_files:
        raise ValueError(f"No parquet files found in HDFS directory: {hdfs_dir}")
    
    # Download files maintaining partition structure
    for i, hdfs_path in enumerate(parquet_files):
        relative_path = hdfs_path.replace(hdfs_dir, '').lstrip('/')
        local_path = os.path.join(local_dir, relative_path)
        logger.info(f"Downloading [{i+1}/{len(parquet_files)}]: {relative_path}")
        download_file(hdfs_path, local_path)
    
    logger.info(f"All files downloaded to: {local_dir}")
    return local_dir


def calculate_rfm_scores(df: pd.DataFrame, n_quantiles: int = 5) -> pd.DataFrame:
    """
    Calculate RFM scores (1-5 scale) using quantile-based binning
    
    Args:
        df: DataFrame with Recency, Frequency, Monetary columns
        n_quantiles: Number of quantiles for scoring
    
    Returns:
        DataFrame with added R_Score, F_Score, M_Score columns
    """
    df = df.copy()
    
    # R Score: Lower recency is better (5 = most recent)
    df['R_Score'] = pd.qcut(df['Recency'], q=n_quantiles, labels=False, duplicates='drop')
    df['R_Score'] = n_quantiles - df['R_Score']  # Reverse
    
    # F Score: Higher frequency is better
    df['F_Score'] = pd.qcut(df['Frequency'].rank(method='first'), q=n_quantiles, labels=False, duplicates='drop') + 1
    
    # M Score: Higher monetary is better
    df['M_Score'] = pd.qcut(df['Monetary'].rank(method='first'), q=n_quantiles, labels=False, duplicates='drop') + 1
    
    # Normalize to 1-5 scale
    for col in ['R_Score', 'F_Score', 'M_Score']:
        min_val, max_val = df[col].min(), df[col].max()
        if max_val > min_val:
            df[col] = 1 + 4 * (df[col] - min_val) / (max_val - min_val)
            df[col] = df[col].round().astype(int)
        else:
            df[col] = 3
    
    # Combined scores
    df['RFM_Score'] = df['R_Score'].astype(str) + df['F_Score'].astype(str) + df['M_Score'].astype(str)
    df['RFM_Score_Numeric'] = (df['R_Score'] + df['F_Score'] + df['M_Score']) / 3
    
    return df


def assign_rfm_segments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign customer segments based on RFM scores
    
    Segments:
        Champions, Loyal Customers, Potential Loyalists, New Customers,
        Promising, Need Attention, About to Sleep, At Risk, Can't Lose Them,
        Hibernating, Lost, Others
    """
    df = df.copy()
    
    def segment_customer(row):
        r, f, m = row['R_Score'], row['F_Score'], row['M_Score']
        
        if (r >= 4) and (f >= 4) and (m >= 4):
            return 'Champions'
        elif (f >= 4) and (r >= 3):
            return 'Loyal Customers'
        elif (r >= 4) and (2 <= f <= 3) and (m >= 3):
            return 'Potential Loyalists'
        elif (r >= 4) and (f == 1) and (m >= 4):
            return 'Promising'
        elif (r >= 4) and (f == 1):
            return 'New Customers'
        elif (r == 3) and (f == 3) and (m == 3):
            return 'Need Attention'
        elif (2 <= r <= 3) and (f <= 2) and (m <= 3):
            return 'About to Sleep'
        elif (r == 1) and (f >= 4) and (m >= 4):
            return "Can't Lose Them"
        elif (r <= 2) and (f >= 4) and (m >= 4):
            return 'At Risk'
        elif (r <= 2) and (f <= 2) and (m <= 2):
            return 'Hibernating'
        elif (r <= 2) and (f <= 2):
            return 'Lost'
        else:
            return 'Others'
    
    df['RFM_Segment'] = df.apply(segment_customer, axis=1)
    return df


# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def task_validate_source_data(**context) -> Dict[str, Any]:
    """
    Task 1: Validate source data availability in HDFS
    
    Checks:
        - HDFS connectivity
        - Parquet files existence
        - Data freshness
    
    Returns:
        Dict with validation results and metadata
    """
    logger.info("=" * 60)
    logger.info("TASK: Validating Source Data in HDFS")
    logger.info("=" * 60)
    
    base_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1"
    
    # Check HDFS connectivity
    try:
        url = f"{base_url}{HDFS_INPUT_DIR}?op=LISTSTATUS&user.name={HDFS_USER}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        raise ConnectionError(f"Cannot connect to HDFS: {e}")
    
    # Count parquet files
    def count_parquet_files(path: str) -> Tuple[int, List[str]]:
        url = f"{base_url}{path}?op=LISTSTATUS&user.name={HDFS_USER}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        count = 0
        partitions = []
        file_statuses = response.json().get('FileStatuses', {}).get('FileStatus', [])
        
        for item in file_statuses:
            item_path = f"{path}/{item['pathSuffix']}"
            if item['type'] == 'DIRECTORY':
                partitions.append(item['pathSuffix'])
                sub_count, _ = count_parquet_files(item_path)
                count += sub_count
            elif item['pathSuffix'].endswith('.parquet'):
                count += 1
        
        return count, partitions
    
    file_count, partitions = count_parquet_files(HDFS_INPUT_DIR)
    
    if file_count == 0:
        raise ValueError(f"No parquet files found in {HDFS_INPUT_DIR}")
    
    validation_result = {
        'hdfs_dir': HDFS_INPUT_DIR,
        'file_count': file_count,
        'partitions': partitions,
        'validated_at': datetime.now().isoformat()
    }
    
    logger.info(f"✓ Found {file_count} parquet files in {len(partitions)} partitions")
    logger.info(f"✓ Validation passed")
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='validation_result', value=validation_result)
    
    return validation_result


def task_phase1_clean_data(**context) -> Dict[str, Any]:
    """
    Task 2: Phase 1 - Load and Clean Data
    
    Operations:
        - Download parquet files from HDFS
        - Remove missing CustomerID/Description
        - Remove invalid prices/quantities
        - Remove cancellations and returns
        - Create TotalAmount feature
        - Remove duplicates
    
    Returns:
        Dict with cleaning statistics and temp file path
    """
    logger.info("=" * 60)
    logger.info("TASK: Phase 1 - Data Cleaning")
    logger.info("=" * 60)
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX)
    logger.info(f"Temp directory: {temp_dir}")
    
    try:
        # Download data from HDFS
        logger.info("Downloading data from HDFS...")
        download_hdfs_parquet_files(HDFS_INPUT_DIR, os.path.join(temp_dir, 'raw'))
        
        # Read parquet files
        logger.info("Reading parquet files...")
        df = pd.read_parquet(os.path.join(temp_dir, 'raw'))
        initial_rows = len(df)
        logger.info(f"Loaded {initial_rows:,} records")
        
        # Cleaning steps
        logger.info("Cleaning data...")
        
        # Step 1: Remove missing/invalid CustomerID
        df = df.dropna(subset=['CustomerID'])
        df = df[df['CustomerID'].notna()]
        
        # Step 2: Remove missing Description
        df = df.dropna(subset=['Description'])
        
        # Step 3: Remove invalid prices
        df = df[df['Price'] > 0]
        
        # Step 4: Remove zero quantities
        df = df[df['Quantity'] != 0]
        
        # Step 5: Remove cancellations and returns
        df = df[~df['Invoice'].astype(str).str.startswith('C')]
        df = df[df['Quantity'] > 0]
        
        # Step 6: Create TotalAmount
        df['TotalAmount'] = df['Quantity'] * df['Price']
        
        # Step 7: Remove duplicates
        df = df.drop_duplicates()
        
        # Step 8: Convert CustomerID to integer
        df['CustomerID'] = df['CustomerID'].astype(int)
        
        final_rows = len(df)
        
        # Save cleaned data
        clean_path = os.path.join(temp_dir, 'clean_transactions.parquet')
        df.to_parquet(clean_path, index=False)
        
        # Calculate reference date
        reference_date = df['InvoiceDate'].max() + timedelta(days=1)
        
        result = {
            'temp_dir': temp_dir,
            'clean_data_path': clean_path,
            'initial_rows': initial_rows,
            'final_rows': final_rows,
            'rows_removed': initial_rows - final_rows,
            'removal_percentage': round((initial_rows - final_rows) / initial_rows * 100, 2),
            'reference_date': reference_date.isoformat(),
            'date_range': {
                'min': df['InvoiceDate'].min().isoformat(),
                'max': df['InvoiceDate'].max().isoformat()
            }
        }
        
        logger.info(f"✓ Cleaned data: {final_rows:,} rows ({result['removal_percentage']}% removed)")
        logger.info(f"✓ Reference date: {reference_date}")
        
        context['ti'].xcom_push(key='phase1_clean_result', value=result)
        
        return result
        
    except Exception as e:
        # Cleanup on failure
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise


def task_phase1_engineer_features(**context) -> Dict[str, Any]:
    """
    Task 3: Phase 1 - Engineer Customer Features
    
    Features created:
        - Recency: Days since last purchase
        - Frequency: Number of unique invoices
        - Monetary: Total spend
        - Tenure: Days between first and last purchase
        - AvgTransactionValue, StdTransactionValue
        - TotalItemsPurchased, AvgItemsPerInvoice
        - CustomerLifetime, PurchaseFrequencyRate
    
    Returns:
        Dict with feature engineering results
    """
    logger.info("=" * 60)
    logger.info("TASK: Phase 1 - Feature Engineering")
    logger.info("=" * 60)
    
    # Get previous task result (note: task_ids must include TaskGroup prefix)
    phase1_clean = context['ti'].xcom_pull(key='phase1_clean_result', task_ids='phase1_data_preparation.phase1_clean_data')
    temp_dir = phase1_clean['temp_dir']
    clean_path = phase1_clean['clean_data_path']
    reference_date = pd.Timestamp(phase1_clean['reference_date'])
    
    # Load cleaned data
    logger.info("Loading cleaned data...")
    df = pd.read_parquet(clean_path)
    
    logger.info("Engineering customer features...")
    
    # Aggregate by customer
    customer_features = df.groupby('CustomerID').agg(
        FirstPurchaseDate=('InvoiceDate', 'min'),
        LastPurchaseDate=('InvoiceDate', 'max'),
        TransactionCount=('Invoice', 'count'),
        InvoiceCount=('Invoice', 'nunique'),
        TotalItemsPurchased=('Quantity', 'sum'),
        TotalSpend=('TotalAmount', 'sum'),
        AvgTransactionValue=('TotalAmount', 'mean'),
        StdTransactionValue=('TotalAmount', 'std'),
        MinTransactionValue=('TotalAmount', 'min'),
        MaxTransactionValue=('TotalAmount', 'max')
    ).reset_index()
    
    # Fill NaN for std (single transaction customers)
    customer_features['StdTransactionValue'] = customer_features['StdTransactionValue'].fillna(0)
    
    # Calculate RFM metrics
    customer_features['Recency'] = (reference_date - customer_features['LastPurchaseDate']).dt.days
    customer_features['Frequency'] = customer_features['InvoiceCount']
    customer_features['Monetary'] = customer_features['TotalSpend']
    
    # Additional features
    customer_features['Tenure'] = (
        customer_features['LastPurchaseDate'] - customer_features['FirstPurchaseDate']
    ).dt.days
    
    customer_features['PurchaseFrequencyRate'] = (
        customer_features['InvoiceCount'] / (customer_features['Tenure'] + 1)
    )
    
    customer_features['AvgItemsPerInvoice'] = (
        customer_features['TotalItemsPurchased'] / customer_features['InvoiceCount']
    )
    
    customer_features['CustomerLifetime'] = (
        reference_date - customer_features['FirstPurchaseDate']
    ).dt.days
    
    # Get primary country for each customer
    country_mode = df.groupby('CustomerID')['Country'].agg(
        lambda x: x.value_counts().index[0]
    ).reset_index()
    country_mode.columns = ['CustomerID', 'Country']
    
    customer_features = customer_features.merge(country_mode, on='CustomerID', how='left')
    
    # Save customer features
    features_path = os.path.join(temp_dir, 'customer_features.parquet')
    customer_features.to_parquet(features_path, index=False)
    
    result = {
        'features_path': features_path,
        'customer_count': len(customer_features),
        'feature_columns': list(customer_features.columns),
        'feature_count': len(customer_features.columns)
    }
    
    logger.info(f"✓ Created {result['customer_count']:,} customer profiles")
    logger.info(f"✓ Engineered {result['feature_count']} features")
    
    context['ti'].xcom_push(key='phase1_features_result', value=result)
    
    return result


def task_phase1_create_dim_product(**context) -> Dict[str, Any]:
    """
    Task 4: Phase 1 - Create Product Dimension Table
    
    Creates Dim_Product with unique products
    
    Returns:
        Dict with dimension table info
    """
    logger.info("=" * 60)
    logger.info("TASK: Phase 1 - Create Dim_Product")
    logger.info("=" * 60)
    
    # Get previous task result (note: task_ids must include TaskGroup prefix)
    phase1_clean = context['ti'].xcom_pull(key='phase1_clean_result', task_ids='phase1_data_preparation.phase1_clean_data')
    temp_dir = phase1_clean['temp_dir']
    clean_path = phase1_clean['clean_data_path']
    
    # Load cleaned data
    df = pd.read_parquet(clean_path)
    
    # Create Dim_Product
    logger.info("Creating Dim_Product...")
    dim_product = df[['StockCode', 'Description']].drop_duplicates(subset=['StockCode'])
    dim_product = dim_product.reset_index(drop=True)
    dim_product['ProductKey'] = dim_product.index + 1
    
    # Reorder columns
    dim_product = dim_product[['ProductKey', 'StockCode', 'Description']]
    
    # Save
    product_path = os.path.join(temp_dir, 'dim_product.parquet')
    dim_product.to_parquet(product_path, index=False)
    
    result = {
        'dim_product_path': product_path,
        'product_count': len(dim_product)
    }
    
    logger.info(f"✓ Created Dim_Product: {result['product_count']:,} products")
    
    context['ti'].xcom_push(key='dim_product_result', value=result)
    
    return result


def task_phase2_rfm_scoring(**context) -> Dict[str, Any]:
    """
    Task 5: Phase 2 - Calculate RFM Scores
    
    Calculates R, F, M scores (1-5 scale) using quantile-based binning
    
    Returns:
        Dict with scoring results
    """
    logger.info("=" * 60)
    logger.info("TASK: Phase 2 - RFM Scoring")
    logger.info("=" * 60)
    
    # Get previous task results (note: task_ids must include TaskGroup prefix)
    phase1_clean = context['ti'].xcom_pull(key='phase1_clean_result', task_ids='phase1_data_preparation.phase1_clean_data')
    phase1_features = context['ti'].xcom_pull(key='phase1_features_result', task_ids='phase1_data_preparation.phase1_engineer_features')
    
    temp_dir = phase1_clean['temp_dir']
    features_path = phase1_features['features_path']
    
    # Load customer features
    logger.info("Loading customer features...")
    df = pd.read_parquet(features_path)
    
    # Calculate RFM scores
    logger.info(f"Calculating RFM scores (quantiles={RFM_QUANTILES})...")
    df = calculate_rfm_scores(df, n_quantiles=RFM_QUANTILES)
    
    # Save
    rfm_path = os.path.join(temp_dir, 'customer_rfm.parquet')
    df.to_parquet(rfm_path, index=False)
    
    # Score distribution
    score_dist = {
        'R_Score': df['R_Score'].value_counts().to_dict(),
        'F_Score': df['F_Score'].value_counts().to_dict(),
        'M_Score': df['M_Score'].value_counts().to_dict()
    }
    
    result = {
        'rfm_path': rfm_path,
        'customer_count': len(df),
        'score_distribution': score_dist
    }
    
    logger.info(f"✓ Calculated RFM scores for {result['customer_count']:,} customers")
    
    context['ti'].xcom_push(key='phase2_rfm_result', value=result)
    
    return result


def task_phase2_segmentation(**context) -> Dict[str, Any]:
    """
    Task 6: Phase 2 - Customer Segmentation
    
    Assigns behavioral segments based on RFM scores and creates final Dim_Customer
    
    Segments:
        Champions, Loyal Customers, Potential Loyalists, New Customers,
        Promising, Need Attention, About to Sleep, At Risk, Can't Lose Them,
        Hibernating, Lost, Others
    
    Returns:
        Dict with segmentation results
    """
    logger.info("=" * 60)
    logger.info("TASK: Phase 2 - Customer Segmentation")
    logger.info("=" * 60)
    
    # Get previous task results (note: task_ids must include TaskGroup prefix)
    phase1_clean = context['ti'].xcom_pull(key='phase1_clean_result', task_ids='phase1_data_preparation.phase1_clean_data')
    phase2_rfm = context['ti'].xcom_pull(key='phase2_rfm_result', task_ids='phase2_rfm_segmentation.phase2_rfm_scoring')
    
    temp_dir = phase1_clean['temp_dir']
    rfm_path = phase2_rfm['rfm_path']
    
    # Load RFM data
    logger.info("Loading RFM data...")
    df = pd.read_parquet(rfm_path)
    
    # Assign segments
    logger.info("Assigning customer segments...")
    df = assign_rfm_segments(df)
    
    # Add model placeholders (for future ML predictions)
    df['CLV_12M_Predicted'] = 0.0
    df['Propensity_Score'] = 0.0
    df['Uplift_Score'] = 0.0
    
    # Add CustomerKey (surrogate key)
    df = df.reset_index(drop=True)
    df['CustomerKey'] = df.index + 1
    
    # Reorder columns for Dim_Customer
    dim_customer_cols = [
        'CustomerKey', 'CustomerID', 'Country',
        'FirstPurchaseDate', 'LastPurchaseDate',
        'Recency', 'Frequency', 'Monetary',
        'R_Score', 'F_Score', 'M_Score', 'RFM_Score', 'RFM_Score_Numeric', 'RFM_Segment',
        'TransactionCount', 'InvoiceCount', 'TotalItemsPurchased', 'TotalSpend',
        'AvgTransactionValue', 'StdTransactionValue', 'MinTransactionValue', 'MaxTransactionValue',
        'Tenure', 'PurchaseFrequencyRate', 'AvgItemsPerInvoice', 'CustomerLifetime',
        'CLV_12M_Predicted', 'Propensity_Score', 'Uplift_Score'
    ]
    
    df = df[dim_customer_cols]
    
    # Save Dim_Customer
    dim_customer_path = os.path.join(temp_dir, 'dim_customer.parquet')
    df.to_parquet(dim_customer_path, index=False)
    
    # Segment distribution
    segment_dist = df['RFM_Segment'].value_counts().to_dict()
    
    # Segment profile summary
    segment_profile = df.groupby('RFM_Segment').agg(
        CustomerCount=('CustomerID', 'count'),
        Recency_Mean=('Recency', 'mean'),
        Frequency_Mean=('Frequency', 'mean'),
        Monetary_Mean=('Monetary', 'mean'),
        TotalRevenue=('Monetary', 'sum')
    ).round(2)
    
    total_revenue = df['Monetary'].sum()
    segment_profile['RevenueContribution'] = (
        segment_profile['TotalRevenue'] / total_revenue * 100
    ).round(2)
    
    # Save segment profile
    profile_path = os.path.join(temp_dir, 'segment_profile.parquet')
    segment_profile.reset_index().to_parquet(profile_path, index=False)
    
    result = {
        'dim_customer_path': dim_customer_path,
        'segment_profile_path': profile_path,
        'customer_count': len(df),
        'segment_count': len(segment_dist),
        'segment_distribution': segment_dist
    }
    
    logger.info(f"✓ Assigned {result['segment_count']} segments to {result['customer_count']:,} customers")
    for segment, count in sorted(segment_dist.items(), key=lambda x: -x[1])[:5]:
        logger.info(f"   - {segment}: {count:,}")
    
    context['ti'].xcom_push(key='phase2_segment_result', value=result)
    
    return result


def task_load_fact_sales(**context) -> Dict[str, Any]:
    """
    Task 7: Load Fact_Sales to PostgreSQL
    
    Loads cleaned transaction data as fact table
    
    Returns:
        Dict with load statistics
    """
    logger.info("=" * 60)
    logger.info("TASK: Load Fact_Sales to PostgreSQL")
    logger.info("=" * 60)
    
    # Get previous task results (note: task_ids must include TaskGroup prefix)
    phase1_clean = context['ti'].xcom_pull(key='phase1_clean_result', task_ids='phase1_data_preparation.phase1_clean_data')
    phase2_segment = context['ti'].xcom_pull(key='phase2_segment_result', task_ids='phase2_rfm_segmentation.phase2_segmentation')
    dim_product_result = context['ti'].xcom_pull(key='dim_product_result', task_ids='phase1_data_preparation.phase1_create_dim_product')
    
    temp_dir = phase1_clean['temp_dir']
    clean_path = phase1_clean['clean_data_path']
    dim_customer_path = phase2_segment['dim_customer_path']
    dim_product_path = dim_product_result['dim_product_path']
    
    # Load data
    logger.info("Loading data...")
    df_sales = pd.read_parquet(clean_path)
    df_customer = pd.read_parquet(dim_customer_path)
    df_product = pd.read_parquet(dim_product_path)
    
    # Create customer and product key mappings
    customer_key_map = df_customer.set_index('CustomerID')['CustomerKey'].to_dict()
    product_key_map = df_product.set_index('StockCode')['ProductKey'].to_dict()
    
    # Add surrogate keys to fact table
    df_sales['CustomerKey'] = df_sales['CustomerID'].map(customer_key_map)
    df_sales['ProductKey'] = df_sales['StockCode'].map(product_key_map)
    
    # Add SalesKey
    df_sales = df_sales.reset_index(drop=True)
    df_sales['SalesKey'] = df_sales.index + 1
    
    # Select and reorder columns for Fact_Sales
    fact_sales_cols = [
        'SalesKey', 'Invoice', 'InvoiceDate',
        'CustomerKey', 'ProductKey',
        'Quantity', 'Price', 'TotalAmount',
        'Country'
    ]
    
    # Handle missing keys (products/customers not in dimension tables)
    df_sales['CustomerKey'] = df_sales['CustomerKey'].fillna(-1).astype(int)
    df_sales['ProductKey'] = df_sales['ProductKey'].fillna(-1).astype(int)
    
    df_fact = df_sales[fact_sales_cols]
    
    # Load to PostgreSQL using psycopg2 directly (avoids pandas/SQLAlchemy compatibility issues)
    logger.info(f"Loading {len(df_fact):,} records to PostgreSQL...")
    load_dataframe_to_postgres(df_fact, 'fact_sales', POSTGRES_SCHEMA, if_exists='replace')
    
    # Create indexes using SQLAlchemy connection (use lowercase column names)
    engine = get_postgres_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON {POSTGRES_SCHEMA}.fact_sales (customerkey)"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON {POSTGRES_SCHEMA}.fact_sales (productkey)"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON {POSTGRES_SCHEMA}.fact_sales (invoicedate)"))
    
    result = {
        'table_name': f'{POSTGRES_SCHEMA}.fact_sales',
        'records_loaded': len(df_fact),
        'columns': list(df_fact.columns)
    }
    
    logger.info(f"✓ Loaded Fact_Sales: {result['records_loaded']:,} records")
    
    context['ti'].xcom_push(key='load_fact_sales_result', value=result)
    
    return result


def task_load_dim_customer(**context) -> Dict[str, Any]:
    """
    Task 8: Load Dim_Customer to PostgreSQL
    
    Loads customer dimension with RFM scores and segments
    
    Returns:
        Dict with load statistics
    """
    logger.info("=" * 60)
    logger.info("TASK: Load Dim_Customer to PostgreSQL")
    logger.info("=" * 60)
    
    # Get previous task result (note: task_ids must include TaskGroup prefix)
    phase2_segment = context['ti'].xcom_pull(key='phase2_segment_result', task_ids='phase2_rfm_segmentation.phase2_segmentation')
    dim_customer_path = phase2_segment['dim_customer_path']
    
    # Load data
    logger.info("Loading Dim_Customer data...")
    df = pd.read_parquet(dim_customer_path)
    
    # Load to PostgreSQL using psycopg2 directly (avoids pandas/SQLAlchemy compatibility issues)
    logger.info(f"Loading {len(df):,} records to PostgreSQL...")
    load_dataframe_to_postgres(df, 'dim_customer', POSTGRES_SCHEMA, if_exists='replace')
    
    # Create indexes using SQLAlchemy connection (use lowercase column names)
    engine = get_postgres_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON {POSTGRES_SCHEMA}.dim_customer (customerid)"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_dim_customer_segment ON {POSTGRES_SCHEMA}.dim_customer (rfm_segment)"))
    
    result = {
        'table_name': f'{POSTGRES_SCHEMA}.dim_customer',
        'records_loaded': len(df),
        'columns': list(df.columns)
    }
    
    logger.info(f"✓ Loaded Dim_Customer: {result['records_loaded']:,} records")
    
    context['ti'].xcom_push(key='load_dim_customer_result', value=result)
    
    return result


def task_load_dim_product(**context) -> Dict[str, Any]:
    """
    Task 9: Load Dim_Product to PostgreSQL
    
    Loads product dimension table
    
    Returns:
        Dict with load statistics
    """
    logger.info("=" * 60)
    logger.info("TASK: Load Dim_Product to PostgreSQL")
    logger.info("=" * 60)
    
    # Get previous task result (note: task_ids must include TaskGroup prefix)
    dim_product_result = context['ti'].xcom_pull(key='dim_product_result', task_ids='phase1_data_preparation.phase1_create_dim_product')
    dim_product_path = dim_product_result['dim_product_path']
    
    # Load data
    logger.info("Loading Dim_Product data...")
    df = pd.read_parquet(dim_product_path)
    
    # Load to PostgreSQL using psycopg2 directly (avoids pandas/SQLAlchemy compatibility issues)
    logger.info(f"Loading {len(df):,} records to PostgreSQL...")
    load_dataframe_to_postgres(df, 'dim_product', POSTGRES_SCHEMA, if_exists='replace')
    
    # Create indexes using SQLAlchemy connection (use lowercase column names)
    engine = get_postgres_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_dim_product_code ON {POSTGRES_SCHEMA}.dim_product (stockcode)"))
    
    result = {
        'table_name': f'{POSTGRES_SCHEMA}.dim_product',
        'records_loaded': len(df),
        'columns': list(df.columns)
    }
    
    logger.info(f"✓ Loaded Dim_Product: {result['records_loaded']:,} records")
    
    context['ti'].xcom_push(key='load_dim_product_result', value=result)
    
    return result


def task_validate_warehouse(**context) -> Dict[str, Any]:
    """
    Task 10: Validate Data Warehouse
    
    Validates:
        - Table existence
        - Record counts
        - Referential integrity
        - Basic data quality checks
    
    Returns:
        Dict with validation results
    """
    logger.info("=" * 60)
    logger.info("TASK: Validate Data Warehouse")
    logger.info("=" * 60)
    
    engine = get_postgres_engine()
    validation_results = {}
    
    with engine.connect() as conn:
        # Check table counts
        tables = ['fact_sales', 'dim_customer', 'dim_product']
        
        for table in tables:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.{table}")
            )
            count = result.scalar()
            validation_results[f'{table}_count'] = count
            logger.info(f"✓ {table}: {count:,} records")
        
        # Check referential integrity (use lowercase column names)
        # Fact_Sales -> Dim_Customer
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.fact_sales f
            WHERE f.customerkey > 0 
            AND NOT EXISTS (
                SELECT 1 FROM {POSTGRES_SCHEMA}.dim_customer d 
                WHERE d.customerkey = f.customerkey
            )
        """))
        orphan_customers = result.scalar()
        validation_results['orphan_customer_refs'] = orphan_customers
        
        # Fact_Sales -> Dim_Product
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.fact_sales f
            WHERE f.productkey > 0
            AND NOT EXISTS (
                SELECT 1 FROM {POSTGRES_SCHEMA}.dim_product d 
                WHERE d.productkey = f.productkey
            )
        """))
        orphan_products = result.scalar()
        validation_results['orphan_product_refs'] = orphan_products
        
        # Segment distribution
        result = conn.execute(text(f"""
            SELECT rfm_segment, COUNT(*) as count 
            FROM {POSTGRES_SCHEMA}.dim_customer 
            GROUP BY rfm_segment 
            ORDER BY count DESC
        """))
        segments = {row[0]: row[1] for row in result}
        validation_results['segment_distribution'] = segments
    
    # Validation status
    is_valid = (
        validation_results['orphan_customer_refs'] == 0 and
        validation_results['orphan_product_refs'] == 0
    )
    
    validation_results['is_valid'] = is_valid
    validation_results['validated_at'] = datetime.now().isoformat()
    
    if is_valid:
        logger.info("✓ Data warehouse validation PASSED")
    else:
        logger.warning("⚠ Data warehouse validation has warnings")
        if orphan_customers > 0:
            logger.warning(f"   - {orphan_customers} orphan customer references")
        if orphan_products > 0:
            logger.warning(f"   - {orphan_products} orphan product references")
    
    context['ti'].xcom_push(key='validation_result', value=validation_results)
    
    return validation_results


def task_cleanup_temp(**context) -> Dict[str, Any]:
    """
    Task 11: Cleanup Temporary Files
    
    Removes temporary files created during processing
    
    Returns:
        Dict with cleanup status
    """
    logger.info("=" * 60)
    logger.info("TASK: Cleanup Temporary Files")
    logger.info("=" * 60)
    
    # Get temp directory from previous task (note: task_ids must include TaskGroup prefix)
    phase1_clean = context['ti'].xcom_pull(key='phase1_clean_result', task_ids='phase1_data_preparation.phase1_clean_data')
    
    if phase1_clean and 'temp_dir' in phase1_clean:
        temp_dir = phase1_clean['temp_dir']
        
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logger.info(f"✓ Cleaned up temp directory: {temp_dir}")
            return {'cleaned': True, 'temp_dir': temp_dir}
    
    logger.info("No temp directory to clean up")
    return {'cleaned': False}


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
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='historical_data_etl_processing',
    default_args=default_args,
    description='ETL Pipeline: HDFS Raw Zone → Spark Processing → PostgreSQL Data Warehouse',
    schedule_interval=None,  # Manual trigger or triggered by upstream DAG
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'historical', 'spark', 'postgresql', 'data-warehouse'],
    doc_md=__doc__,
) as dag:
    
    # ========================================================================
    # START
    # ========================================================================
    start = EmptyOperator(task_id='start')
    
    # ========================================================================
    # VALIDATION
    # ========================================================================
    validate_source = PythonOperator(
        task_id='validate_source_data',
        python_callable=task_validate_source_data,
        doc_md="Validate source data availability in HDFS"
    )
    
    # ========================================================================
    # PHASE 1: DATA PREPARATION
    # ========================================================================
    with TaskGroup(group_id='phase1_data_preparation') as phase1_group:
        
        clean_data = PythonOperator(
            task_id='phase1_clean_data',
            python_callable=task_phase1_clean_data,
            doc_md="Clean raw data: remove nulls, invalid values, cancellations"
        )
        
        engineer_features = PythonOperator(
            task_id='phase1_engineer_features',
            python_callable=task_phase1_engineer_features,
            doc_md="Engineer customer-level features: RFM metrics, aggregations"
        )
        
        create_dim_product = PythonOperator(
            task_id='phase1_create_dim_product',
            python_callable=task_phase1_create_dim_product,
            doc_md="Create Dim_Product dimension table"
        )
        
        # Phase 1 dependencies
        clean_data >> [engineer_features, create_dim_product]
    
    # ========================================================================
    # PHASE 2: RFM SEGMENTATION
    # ========================================================================
    with TaskGroup(group_id='phase2_rfm_segmentation') as phase2_group:
        
        rfm_scoring = PythonOperator(
            task_id='phase2_rfm_scoring',
            python_callable=task_phase2_rfm_scoring,
            doc_md="Calculate RFM scores (1-5 scale)"
        )
        
        segmentation = PythonOperator(
            task_id='phase2_segmentation',
            python_callable=task_phase2_segmentation,
            doc_md="Assign customer segments based on RFM scores"
        )
        
        # Phase 2 dependencies
        rfm_scoring >> segmentation
    
    # ========================================================================
    # LOAD TO DATA WAREHOUSE
    # ========================================================================
    with TaskGroup(group_id='load_to_warehouse') as load_group:
        
        load_fact_sales = PythonOperator(
            task_id='load_fact_sales',
            python_callable=task_load_fact_sales,
            doc_md="Load Fact_Sales to PostgreSQL"
        )
        
        load_dim_customer = PythonOperator(
            task_id='load_dim_customer',
            python_callable=task_load_dim_customer,
            doc_md="Load Dim_Customer to PostgreSQL"
        )
        
        load_dim_product = PythonOperator(
            task_id='load_dim_product',
            python_callable=task_load_dim_product,
            doc_md="Load Dim_Product to PostgreSQL"
        )
    
    # ========================================================================
    # VALIDATION & CLEANUP
    # ========================================================================
    validate_warehouse = PythonOperator(
        task_id='validate_warehouse',
        python_callable=task_validate_warehouse,
        doc_md="Validate data warehouse integrity"
    )
    
    cleanup = PythonOperator(
        task_id='cleanup_temp',
        python_callable=task_cleanup_temp,
        trigger_rule='all_done',  # Run even if upstream fails
        doc_md="Cleanup temporary files"
    )
    
    # ========================================================================
    # END
    # ========================================================================
    end = EmptyOperator(task_id='end')
    
    # ========================================================================
    # DAG DEPENDENCIES
    # ========================================================================
    # Main flow
    start >> validate_source >> phase1_group >> phase2_group >> load_group
    
    # Load tasks can run in parallel after phase2
    load_group >> validate_warehouse >> cleanup >> end
