"""
DAG để crawl dữ liệu Alcohol Consumption từ FiveThirtyEight GitHub
và lưu vào HDFS Data Lake raw_zone/external_alcohol via WebHDFS API
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import logging
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cấu hình
# URL dữ liệu Alcohol Consumption từ FiveThirtyEight GitHub
ALCOHOL_DATA_URL = "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv"

# WebHDFS Configuration - Use host IP accessible from Docker container
WEBHDFS_HOST = os.environ.get('WEBHDFS_HOST', "192.168.1.6")
WEBHDFS_PORT = os.environ.get('WEBHDFS_PORT', "9870")
HDFS_USER = os.environ.get('HDFS_USER', "anhth")
HDFS_OUTPUT_DIR = "/data_lake/raw_zone/external_alcohol"

# Cấu hình logging
logger = logging.getLogger(__name__)


def upload_to_webhdfs(local_file: str, hdfs_path: str) -> bool:
    """
    Upload file to HDFS using WebHDFS REST API
    
    Args:
        local_file: Path to local file
        hdfs_path: HDFS destination path (e.g., /data_lake/raw_zone/file.parquet)
    
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
            # The redirect URL contains hostname like 'brick-server-a6k' which Docker can't resolve
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(datanode_url)
            # Replace hostname with the known host IP, keep the port (9864)
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


def crawl_alcohol_data():
    """
    Crawl (đọc) dữ liệu Alcohol Consumption từ FiveThirtyEight GitHub API.
    Dữ liệu bao gồm thống kê lượng tiêu thụ bia, rượu vang, rượu mạnh theo từng quốc gia.
    
    Returns:
        pandas.DataFrame: DataFrame chứa dữ liệu alcohol consumption
    """
    logger.info(f"Bắt đầu crawl dữ liệu từ: {ALCOHOL_DATA_URL}")
    
    try:
        # Gọi API GitHub để lấy dữ liệu CSV
        response = requests.get(ALCOHOL_DATA_URL, timeout=60)
        response.raise_for_status()
        
        # Đọc CSV từ response content
        from io import StringIO
        csv_content = StringIO(response.text)
        df = pd.read_csv(csv_content)
        
        logger.info(f"Đã crawl thành công {len(df)} records")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Sample data:\n{df.head()}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi crawl dữ liệu: {str(e)}")
        raise


def extract_alcohol_data(**context):
    """
    Airflow Task để extract dữ liệu Alcohol Consumption từ GitHub
    và lưu vào HDFS raw_zone/external_alcohol dưới dạng Parquet via WebHDFS
    """
    logger.info("Bắt đầu task extract_alcohol_data")
    
    # Crawl dữ liệu từ GitHub
    df = crawl_alcohol_data()
    
    if df is None or df.empty:
        logger.warning("Không có dữ liệu để lưu")
        return {"status": "no_data", "total_records": 0}
    
    # Tạo tên file với timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"alcohol_consumption_{timestamp}.parquet"
    
    # Lưu tạm vào local trước khi upload lên HDFS
    with tempfile.TemporaryDirectory() as temp_dir:
        local_file = os.path.join(temp_dir, filename)
        
        # Lưu thành file Parquet tạm
        df.to_parquet(local_file, index=False, engine='pyarrow')
        
        file_size_kb = os.path.getsize(local_file) / 1024
        
        # Upload lên HDFS via WebHDFS
        hdfs_path = f"{HDFS_OUTPUT_DIR}/{filename}"
        if not upload_to_webhdfs(local_file, hdfs_path):
            raise Exception(f"Failed to upload file to HDFS: {hdfs_path}")
        
        logger.info(f"Đã lưu {len(df)} records vào HDFS: {hdfs_path} ({file_size_kb:.2f} KB)")
    
    # Log thống kê cơ bản
    logger.info(f"Số quốc gia: {df['country'].nunique()}")
    logger.info(f"Tổng beer_servings: {df['beer_servings'].sum()}")
    logger.info(f"Tổng spirit_servings: {df['spirit_servings'].sum()}")
    logger.info(f"Tổng wine_servings: {df['wine_servings'].sum()}")
    
    return {
        "status": "success",
        "total_records": len(df),
        "hdfs_path": hdfs_path,
        "file_size_kb": file_size_kb
    }


# Định nghĩa default arguments cho DAG
default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
with DAG(
    dag_id='extract_alcohol_data',
    default_args=default_args,
    description='Crawl dữ liệu Alcohol Consumption từ FiveThirtyEight và lưu vào HDFS Data Lake',
    schedule_interval=None,  # Chạy thủ công
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['extract', 'external', 'alcohol', 'hdfs'],
) as dag:
    
    # Task crawl và lưu dữ liệu
    extract_task = PythonOperator(
        task_id='extract_alcohol_data',
        python_callable=extract_alcohol_data,
        provide_context=True,
    )
    
    extract_task
