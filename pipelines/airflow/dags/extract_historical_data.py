"""
DAG để extract dữ liệu historical từ API Internal
và lưu vào HDFS Data Lake raw_zone/internal_historical via WebHDFS API
Dữ liệu được phân vùng theo tháng (year/month)
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import logging
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cấu hình API
API_BASE_URL = os.environ.get('API_BASE_URL', "http://data_gov_api:80/api/v1/raw_transactions")
API_TOKEN = os.environ.get('API_KEY', "")
BATCH_SIZE = 100  # Số records mỗi request
END_DATE = "2011-11-30"

# WebHDFS Configuration - Use host IP accessible from Docker container
WEBHDFS_HOST = os.environ.get('WEBHDFS_HOST', "192.168.1.6")
WEBHDFS_PORT = os.environ.get('WEBHDFS_PORT', "9870")
HDFS_USER = os.environ.get('HDFS_USER', "anhth")
HDFS_OUTPUT_DIR = "/data_lake/raw_zone/internal_historical"

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


def extract_all_historical_data(**context):
    """
    Lấy toàn bộ dữ liệu lịch sử từ API bằng cách pagination
    và lưu thành file Parquet theo tháng vào HDFS via WebHDFS
    """
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}"
    }
    
    all_records = []
    offset = 0
    total_fetched = 0
    
    logger.info(f"Bắt đầu extract dữ liệu từ API với end_date={END_DATE}")
    
    # Step 1: Fetch all data from API with pagination
    while True:
        # Tạo URL với pagination
        url = f"{API_BASE_URL}?limit={BATCH_SIZE}&offset={offset}&end_date={END_DATE}"
        
        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # Kiểm tra cấu trúc response
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Nếu API trả về dict với key chứa data
                records = data.get("data", data.get("records", data.get("items", [])))
                if not isinstance(records, list):
                    records = [data]
            else:
                records = []
            
            # Nếu không còn records nào thì dừng
            if not records:
                logger.info(f"Không còn records nào. Tổng số đã fetch: {total_fetched}")
                break
            
            all_records.extend(records)
            total_fetched += len(records)
            
            logger.info(f"Đã fetch {len(records)} records (offset={offset}). Tổng: {total_fetched}")
            
            # Nếu số records trả về ít hơn batch size, nghĩa là đã hết data
            if len(records) < BATCH_SIZE:
                logger.info(f"Đã fetch hết dữ liệu. Tổng số records: {total_fetched}")
                break
            
            offset += BATCH_SIZE
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi gọi API tại offset={offset}: {str(e)}")
            raise
    
    if not all_records:
        logger.warning("Không có dữ liệu nào được fetch từ API")
        return {"status": "no_data", "total_records": 0}
    
    # Step 2: Convert to DataFrame and process
    df = pd.DataFrame(all_records)
    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"DataFrame columns: {df.columns.tolist()}")
    
    # Chuyển đổi InvoiceDate sang datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    
    # Tạo cột year_month để partition theo tháng (format: YYYY-MM)
    df['year_month'] = df['InvoiceDate'].dt.to_period('M').astype(str)
    
    # Step 3: Save to HDFS partitioned by month
    saved_files = []
    total_saved = 0
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for year_month, group_df in df.groupby('year_month'):
            try:
                # Bỏ cột year_month trước khi lưu
                group_df_to_save = group_df.drop(columns=['year_month'])
                
                # Parse year and month from year_month string (e.g., "2010-01")
                year, month = year_month.split('-')
                
                # Tạo partition path theo Hive-style partitioning
                partition_dir = f"{HDFS_OUTPUT_DIR}/year={year}/month={month}"
                filename = f"transactions_{year_month}.parquet"
                
                # Ensure HDFS directory exists
                if not ensure_hdfs_directory(partition_dir):
                    logger.warning(f"Could not ensure directory {partition_dir}, continuing anyway...")
                
                # Lưu tạm vào local
                local_file = os.path.join(temp_dir, filename)
                group_df_to_save.to_parquet(local_file, index=False, engine='pyarrow')
                
                file_size_kb = os.path.getsize(local_file) / 1024
                records_count = len(group_df_to_save)
                
                # Upload lên HDFS via WebHDFS
                hdfs_path = f"{partition_dir}/{filename}"
                if upload_to_webhdfs(local_file, hdfs_path):
                    saved_files.append({
                        "hdfs_path": hdfs_path,
                        "year_month": year_month,
                        "records": records_count,
                        "size_kb": round(file_size_kb, 2)
                    })
                    total_saved += records_count
                    logger.info(f"Đã lưu {records_count} records vào HDFS: {hdfs_path} ({file_size_kb:.2f} KB)")
                else:
                    logger.error(f"Failed to save {hdfs_path}")
                
            except Exception as e:
                logger.error(f"Error processing {year_month}: {str(e)}")
                continue
    
    logger.info(f"Hoàn thành! Đã lưu tổng cộng {total_saved} records vào {len(saved_files)} files trên HDFS")
    
    return {
        "status": "success",
        "total_records": total_fetched,
        "total_saved": total_saved,
        "total_files": len(saved_files),
        "files": saved_files
    }


# Định nghĩa default arguments cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
with DAG(
    dag_id='extract_historical_data',
    default_args=default_args,
    description='Extract historical transaction data từ API Internal và lưu vào HDFS Data Lake',
    schedule_interval=None,  # Chạy thủ công
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['extract', 'internal', 'historical', 'hdfs'],
) as dag:
    
    # Task extract và lưu dữ liệu
    extract_task = PythonOperator(
        task_id='extract_all_historical_data',
        python_callable=extract_all_historical_data,
        provide_context=True,
        execution_timeout=timedelta(hours=2),  # Timeout dài vì có nhiều tháng cần xử lý
    )
    
    extract_task
