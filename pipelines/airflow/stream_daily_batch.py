"""
DAG để giả lập Streaming Data Ingestion
Lấy dữ liệu giao dịch theo ngày từ 6 tháng cuối (2011-07-01 đến 2011-12-09)
và lưu vào Data Lake dưới dạng Parquet theo ngày
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cấu hình
API_BASE_URL = "http://data_gov_api:80/api/v1/raw_transactions"
API_TOKEN = "sk_9f9d4af6075324d188c58252687db3da32a255a2fe528d8230b07e41e112c251"
OUTPUT_DIR = "/opt/airflow/DATA_LAKE/raw_zone/internal_streaming"
BATCH_SIZE = 100  # Số records mỗi request

# Khoảng thời gian dữ liệu streaming (9 ngày cuối của Online Retail II)
STREAMING_START_DATE = datetime(2011, 12, 1)
STREAMING_END_DATE = datetime(2011, 12, 9)  # Ngày cuối cùng có dữ liệu

# Cấu hình logging
logger = logging.getLogger(__name__)


def get_simulated_date(execution_date: datetime) -> datetime:
    """
    Chuyển đổi execution_date của Airflow sang ngày dữ liệu giả lập.
    Mỗi ngày chạy DAG sẽ tương ứng với 1 ngày trong khoảng streaming data.
    
    Ví dụ: 
    - DAG start_date = 2025-01-01 -> lấy data ngày 2011-07-01
    - DAG run ngày 2025-01-02 -> lấy data ngày 2011-07-02
    - ...
    """
    # Tính số ngày từ khi DAG bắt đầu
    dag_start = datetime(2025, 1, 1)
    days_since_start = (execution_date.replace(tzinfo=None) - dag_start).days
    
    # Tính ngày dữ liệu tương ứng
    simulated_date = STREAMING_START_DATE + timedelta(days=days_since_start)
    
    # Đảm bảo không vượt quá ngày kết thúc
    if simulated_date > STREAMING_END_DATE:
        simulated_date = STREAMING_END_DATE
    
    return simulated_date


def fetch_daily_data(target_date: datetime) -> list:
    """
    Lấy dữ liệu giao dịch của 1 ngày cụ thể từ API
    """
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}"
    }
    
    # Format ngày cho API: YYYY-MM-DD
    start_date = target_date.strftime("%Y-%m-%d")
    end_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    all_records = []
    offset = 0
    
    logger.info(f"Bắt đầu fetch dữ liệu cho ngày {start_date}")
    
    while True:
        url = f"{API_BASE_URL}?limit={BATCH_SIZE}&offset={offset}&start_date={start_date}&end_date={end_date}"
        
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
            
            if len(records) < BATCH_SIZE:
                break
            
            offset += BATCH_SIZE
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi gọi API: {str(e)}")
            raise
    
    logger.info(f"Đã fetch {len(all_records)} records cho ngày {start_date}")
    return all_records


def stream_daily_batch(**context):
    """
    Task chính: Lấy dữ liệu của ngày hiện tại (giả lập) và lưu vào Data Lake
    """
    # Lấy execution_date từ Airflow context
    execution_date = context['execution_date']
    
    # Chuyển đổi sang ngày dữ liệu giả lập
    target_date = get_simulated_date(execution_date)
    
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"Target data date (simulated): {target_date.strftime('%Y-%m-%d')}")
    
    # Kiểm tra nếu đã vượt quá khoảng dữ liệu
    if target_date > STREAMING_END_DATE:
        logger.warning(f"Đã vượt quá ngày kết thúc dữ liệu ({STREAMING_END_DATE.strftime('%Y-%m-%d')}). Bỏ qua.")
        return {
            "status": "skipped",
            "reason": "No more data to stream",
            "target_date": target_date.strftime('%Y-%m-%d')
        }
    
    # Fetch dữ liệu từ API
    records = fetch_daily_data(target_date)
    
    if not records:
        logger.warning(f"Không có dữ liệu cho ngày {target_date.strftime('%Y-%m-%d')}")
        return {
            "status": "no_data",
            "target_date": target_date.strftime('%Y-%m-%d'),
            "total_records": 0
        }
    
    # Chuyển đổi sang DataFrame
    df = pd.DataFrame(records)
    logger.info(f"DataFrame shape: {df.shape}")
    
    # Chuyển đổi InvoiceDate sang datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    
    # Tạo thư mục output nếu chưa tồn tại
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Tạo tên file theo ngày: batch_daily_YYYYMMDD.parquet
    date_str = target_date.strftime("%Y%m%d")
    output_file = os.path.join(OUTPUT_DIR, f"batch_daily_{date_str}.parquet")
    
    # Lưu thành file Parquet
    df.to_parquet(output_file, index=False, engine='pyarrow')
    
    file_size_kb = os.path.getsize(output_file) / 1024
    logger.info(f"Đã lưu {len(records)} records vào {output_file} ({file_size_kb:.2f} KB)")
    
    return {
        "status": "success",
        "target_date": target_date.strftime('%Y-%m-%d'),
        "total_records": len(records),
        "output_file": output_file,
        "file_size_kb": round(file_size_kb, 2)
    }


# Định nghĩa default args cho DAG
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
    dag_id='stream_daily_batch',
    default_args=default_args,
    description='Simulate streaming data ingestion - fetch daily transaction data and save to Data Lake',
    schedule_interval='@daily',  # Chạy hàng ngày
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 1, 10),  # Kết thúc sau 9 ngày (tương ứng 9 ngày dữ liệu: 2011-12-01 đến 2011-12-09)
    catchup=True,  # Cho phép backfill để chạy các ngày trước
    max_active_runs=1,  # Chỉ chạy 1 DAG run tại một thời điểm
    tags=['streaming', 'daily', 'data_lake'],
) as dag:
    
    stream_task = PythonOperator(
        task_id='stream_daily_batch',
        python_callable=stream_daily_batch,
        provide_context=True,
    )
    
    stream_task

