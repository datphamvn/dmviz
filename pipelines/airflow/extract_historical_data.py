"""
DAG để extract dữ liệu lịch sử từ API và lưu vào Data Lake dưới dạng Parquet
Dữ liệu được partition theo tháng (year_month)
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cấu hình
# Sử dụng tên container fastapi (data_gov_api) và port nội bộ (80) thay vì host.docker.internal
API_BASE_URL = "http://data_gov_api:80/api/v1/raw_transactions"
API_TOKEN = "sk_9f9d4af6075324d188c58252687db3da32a255a2fe528d8230b07e41e112c251"
OUTPUT_DIR = "/opt/airflow/DATA_LAKE/raw_zone/internal_historical"
BATCH_SIZE = 100  # Số records mỗi request
END_DATE = "2011-11-30"

# Cấu hình logging
logger = logging.getLogger(__name__)


def extract_all_historical_data(**context):
    """
    Lấy toàn bộ dữ liệu lịch sử từ API bằng cách pagination
    và lưu thành file Parquet theo tháng
    """
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}"
    }
    
    all_records = []
    offset = 0
    total_fetched = 0
    
    logger.info(f"Bắt đầu extract dữ liệu từ API với end_date={END_DATE}")
    
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
    
    # Chuyển đổi sang DataFrame
    df = pd.DataFrame(all_records)
    logger.info(f"DataFrame shape: {df.shape}")
    logger.info(f"DataFrame columns: {df.columns.tolist()}")
    
    # Chuyển đổi InvoiceDate sang datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    
    # Tạo cột year_month để partition theo tháng (format: YYYY-MM)
    df['year_month'] = df['InvoiceDate'].dt.to_period('M').astype(str)
    
    # Tạo thư mục output nếu chưa tồn tại
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Lưu file Parquet theo từng tháng
    saved_files = []
    total_saved = 0
    
    for year_month, group_df in df.groupby('year_month'):
        # Bỏ cột year_month trước khi lưu (không cần thiết trong file)
        group_df_to_save = group_df.drop(columns=['year_month'])
        
        # Tạo tên file theo tháng: transactions_YYYY-MM.parquet
        output_file = os.path.join(OUTPUT_DIR, f"transactions_{year_month}.parquet")
        
        # Lưu thành file Parquet
        group_df_to_save.to_parquet(output_file, index=False, engine='pyarrow')
        
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        records_count = len(group_df_to_save)
        total_saved += records_count
        
        saved_files.append({
            "file": output_file,
            "year_month": year_month,
            "records": records_count,
            "size_mb": round(file_size_mb, 2)
        })
        
        logger.info(f"Đã lưu {records_count} records vào {output_file} ({file_size_mb:.2f} MB)")
    
    logger.info(f"Hoàn thành! Đã lưu tổng cộng {total_saved} records vào {len(saved_files)} files")
    
    return {
        "status": "success",
        "total_records": total_fetched,
        "total_files": len(saved_files),
        "files": saved_files
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
    dag_id='extract_historical_data',
    default_args=default_args,
    description='Extract historical transaction data from API and save to Data Lake as Parquet',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['extract', 'historical', 'data_lake'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_historical_data',
        python_callable=extract_all_historical_data,
        provide_context=True,
    )
    
    extract_task

