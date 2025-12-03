"""
DAG để simulate streaming data ingestion từ API Internal
và lưu vào HDFS Data Lake raw_zone/internal_streaming via WebHDFS API
Mỗi lần chạy sẽ lấy dữ liệu của 1 ngày (simulate daily batch from streaming source)
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import logging
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag

# Cấu hình API
API_BASE_URL = os.environ.get('API_BASE_URL', "http://data_gov_api:80/api/v1/raw_transactions")
API_TOKEN = os.environ.get('API_KEY', "")

# WebHDFS Configuration - Use host IP accessible from Docker container
WEBHDFS_HOST = os.environ.get('WEBHDFS_HOST', "192.168.1.6")
WEBHDFS_PORT = os.environ.get('WEBHDFS_PORT', "9870")
HDFS_USER = os.environ.get('HDFS_USER', "anhth")
HDFS_OUTPUT_DIR = "/data_lake/raw_zone/internal_streaming"

# Cấu hình logging
logger = logging.getLogger(__name__)


def ensure_hdfs_directory(hdfs_dir: str) -> bool:
    """
    Ensure HDFS directory exists using WebHDFS
    """
    try:
        # Check if directory exists
        check_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_dir}?op=GETFILESTATUS&user.name={HDFS_USER}"
        response = requests.get(check_url, timeout=30)
        
        if response.status_code == 200:
            logger.info(f"HDFS directory exists: {hdfs_dir}")
            return True
        
        # Directory doesn't exist, create it
        mkdir_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_dir}?op=MKDIRS&user.name={HDFS_USER}"
        response = requests.put(mkdir_url, timeout=30)
        
        if response.status_code == 200:
            logger.info(f"Created HDFS directory: {hdfs_dir}")
            return True
        else:
            logger.error(f"Failed to create directory. Status: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error ensuring directory: {str(e)}")
        return False


def upload_to_webhdfs(local_file: str, hdfs_path: str) -> bool:
    """
    Upload file to HDFS using WebHDFS REST API
    
    Args:
        local_file: Path to local file
        hdfs_path: HDFS destination path
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Step 1: Create file (get redirect URL to DataNode)
        create_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name={HDFS_USER}"
        logger.info(f"Creating file in HDFS: {hdfs_path}")
        
        # This will return a redirect to the DataNode
        response = requests.put(create_url, allow_redirects=False, timeout=30)
        
        if response.status_code == 307:
            # Get the redirect location (DataNode URL)
            datanode_url = response.headers['Location']
            
            # Replace hostname with IP address (Docker container can't resolve hostname)
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(datanode_url)
            fixed_url = urlunparse(parsed._replace(netloc=f"{WEBHDFS_HOST}:{parsed.port}"))
            logger.info(f"Fixed DataNode URL: {fixed_url}")
            
            # Step 2: Upload file content to DataNode
            with open(local_file, 'rb') as f:
                upload_response = requests.put(fixed_url, data=f, timeout=120)
            
            if upload_response.status_code == 201:
                logger.info(f"Successfully uploaded to HDFS: {hdfs_path}")
                return True
            else:
                logger.error(f"Failed to upload to DataNode. Status: {upload_response.status_code}, Response: {upload_response.text}")
                return False
        else:
            logger.error(f"Failed to create file. Status: {response.status_code}, Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error uploading to HDFS via WebHDFS: {str(e)}")
        return False


def get_simulated_date(context) -> datetime:
    """
    Lấy ngày simulate từ execution_date của DAG.
    Historical data đã extract đến 2011-11-30.
    Streaming sẽ chỉ simulate tháng cuối cùng: 2011-12-01 đến 2011-12-09.
    Ta sẽ map execution_date hiện tại sang khoảng thời gian đó.
    """
    execution_date = context['execution_date']
    
    # Base date cho streaming (tháng cuối cùng sau historical)
    # Historical đã extract đến 2011-11-30, nên streaming bắt đầu từ 2011-12-01
    base_historical_date = datetime(2011, 12, 1)
    
    # Base date khi DAG bắt đầu chạy (ví dụ: 2025-01-01)
    base_dag_date = datetime(2025, 1, 1)
    
    # Tính số ngày offset
    days_offset = (execution_date.replace(tzinfo=None) - base_dag_date).days
    
    # Tính ngày simulate trong dữ liệu gốc
    simulated_date = base_historical_date + timedelta(days=days_offset)
    
    # Giới hạn trong khoảng dữ liệu có sẵn (chỉ tháng 12/2011)
    max_date = datetime(2011, 12, 9)
    if simulated_date > max_date:
        # Cycle lại trong khoảng 9 ngày của tháng 12
        days_in_range = (max_date - base_historical_date).days + 1  # 9 ngày
        simulated_date = base_historical_date + timedelta(days=(days_offset % days_in_range))
    
    return simulated_date


def fetch_daily_data(target_date: datetime) -> pd.DataFrame:
    """
    Fetch dữ liệu transactions của một ngày cụ thể từ API.
    Sử dụng start_date và end_date để lọc theo ngày.
    
    Args:
        target_date: Ngày cần lấy dữ liệu
    
    Returns:
        pandas.DataFrame: DataFrame chứa transactions của ngày đó
    """
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}"
    }
    
    # Lấy dữ liệu của 1 ngày bằng cách set start_date = end_date = target_date
    date_str = target_date.strftime("%Y-%m-%d")
    # Để lấy đúng 1 ngày, end_date cần là ngày tiếp theo
    next_date_str = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    all_records = []
    offset = 0
    batch_size = 100
    
    logger.info(f"Fetching data for date {date_str} from API")
    
    while True:
        # Tạo URL với pagination và date filter
        url = f"{API_BASE_URL}?limit={batch_size}&offset={offset}&start_date={date_str}&end_date={date_str}"
        
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # Kiểm tra cấu trúc response
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = data.get("data", data.get("records", data.get("items", [])))
                if not isinstance(records, list):
                    records = [data]
            else:
                records = []
            
            if not records:
                break
            
            all_records.extend(records)
            logger.info(f"Fetched {len(records)} records (offset={offset})")
            
            if len(records) < batch_size:
                break
            
            offset += batch_size
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {date_str}: {str(e)}")
            raise
    
    if not all_records:
        logger.info(f"No data returned for {date_str}")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_records)
    logger.info(f"Total fetched {len(df)} records for {date_str}")
    
    return df


def stream_daily_batch(**context):
    """
    Airflow Task để simulate streaming daily batch ingestion.
    Lấy dữ liệu của 1 ngày và lưu vào HDFS raw_zone/internal_streaming via WebHDFS
    Phân vùng theo ngày: year=YYYY/month=MM/day=DD/
    """
    logger.info("Bắt đầu task stream_daily_batch")
    
    # Lấy ngày simulate từ execution_date
    simulated_date = get_simulated_date(context)
    logger.info(f"Processing date: {simulated_date.strftime('%Y-%m-%d')}")
    
    # Fetch data từ API
    df = fetch_daily_data(simulated_date)
    
    if df.empty:
        logger.warning(f"No data for {simulated_date.strftime('%Y-%m-%d')}")
        return {
            "status": "no_data",
            "date": simulated_date.strftime("%Y-%m-%d"),
            "total_records": 0
        }
    
    # Tạo partition path theo Hive-style partitioning
    partition_dir = f"{HDFS_OUTPUT_DIR}/year={simulated_date.year}/month={simulated_date.month:02d}/day={simulated_date.day:02d}"
    filename = f"transactions_{simulated_date.strftime('%Y%m%d')}.parquet"
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Ensure HDFS directory exists
        if not ensure_hdfs_directory(partition_dir):
            logger.warning(f"Could not ensure directory {partition_dir}, continuing anyway...")
        
        # Lưu tạm vào local
        local_file = os.path.join(temp_dir, filename)
        df.to_parquet(local_file, index=False, engine='pyarrow')
        
        file_size_kb = os.path.getsize(local_file) / 1024
        
        # Upload lên HDFS via WebHDFS
        hdfs_path = f"{partition_dir}/{filename}"
        if not upload_to_webhdfs(local_file, hdfs_path):
            raise Exception(f"Failed to upload file to HDFS: {hdfs_path}")
        
        logger.info(f"Saved {len(df)} records to {hdfs_path} ({file_size_kb:.2f} KB)")
    
    # Log thống kê
    if 'UnitPrice' in df.columns and 'Quantity' in df.columns:
        total_revenue = (df['UnitPrice'] * df['Quantity']).sum()
        logger.info(f"Total revenue for {simulated_date.strftime('%Y-%m-%d')}: {total_revenue:.2f}")
    
    return {
        "status": "success",
        "date": simulated_date.strftime("%Y-%m-%d"),
        "total_records": len(df),
        "hdfs_path": hdfs_path,
        "file_size_kb": file_size_kb
    }


def trigger_processing_dag(**context):
    """
    Trigger stream_daily_processing DAG với simulated_date từ XCom
    """
    ti = context['ti']
    result = ti.xcom_pull(task_ids='stream_daily_batch')
    
    if not result or result.get('status') == 'no_data':
        logger.info("No data ingested, skipping processing trigger")
        return {"status": "skipped", "reason": "no_data"}
    
    simulated_date = result.get('date')
    logger.info(f"Triggering stream_daily_processing for date: {simulated_date}")
    
    # Trigger the processing DAG
    trigger_dag(
        dag_id='stream_daily_processing',
        run_id=f"triggered_by_stream_batch_{simulated_date}_{context['execution_date'].isoformat()}",
        conf={
            'source': 'api_batch',
            'process_date': simulated_date
        },
        execution_date=None,  # Use current time
        replace_microseconds=False,
    )
    
    return {"status": "triggered", "process_date": simulated_date}


# Định nghĩa default arguments cho DAG
default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Định nghĩa DAG
with DAG(
    dag_id='stream_daily_batch',
    default_args=default_args,
    description='Simulate streaming daily batch ingestion và lưu vào HDFS Data Lake',
    schedule_interval='@daily',  # Chạy hàng ngày
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 1, 9),  # Chỉ chạy 9 ngày (map to 2011-12-01 → 2011-12-09)
    catchup=True,  # Bật catchup để simulate streaming
    max_active_runs=3,  # Cho phép 3 runs đồng thời
    tags=['streaming', 'internal', 'daily', 'hdfs'],
) as dag:
    
    # Task stream daily batch
    stream_task = PythonOperator(
        task_id='stream_daily_batch',
        python_callable=stream_daily_batch,
        provide_context=True,
    )
    
    # Trigger processing DAG after successful ingestion
    # Sử dụng PythonOperator để lấy simulated_date từ XCom
    trigger_processing = PythonOperator(
        task_id='trigger_processing',
        python_callable=trigger_processing_dag,
        provide_context=True,
    )
    
    stream_task >> trigger_processing
