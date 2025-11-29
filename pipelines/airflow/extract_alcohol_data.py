"""
DAG để crawl dữ liệu Alcohol Consumption từ FiveThirtyEight GitHub
và lưu vào Data Lake raw_zone/external_alcohol
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cấu hình
# URL dữ liệu Alcohol Consumption từ FiveThirtyEight GitHub
ALCOHOL_DATA_URL = "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv"
OUTPUT_DIR = "/opt/airflow/DATA_LAKE/raw_zone/external_alcohol"

# Cấu hình logging
logger = logging.getLogger(__name__)


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
    và lưu vào raw_zone/external_alcohol dưới dạng Parquet
    """
    logger.info("Bắt đầu task extract_alcohol_data")
    
    # Crawl dữ liệu từ GitHub
    df = crawl_alcohol_data()
    
    if df is None or df.empty:
        logger.warning("Không có dữ liệu để lưu")
        return {"status": "no_data", "total_records": 0}
    
    # Tạo thư mục output nếu chưa tồn tại
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Tạo tên file với timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"alcohol_consumption_{timestamp}.parquet")
    
    # Lưu thành file Parquet
    df.to_parquet(output_file, index=False, engine='pyarrow')
    
    file_size_kb = os.path.getsize(output_file) / 1024
    logger.info(f"Đã lưu {len(df)} records vào {output_file} ({file_size_kb:.2f} KB)")
    
    # Log thống kê cơ bản
    logger.info(f"Số quốc gia: {df['country'].nunique()}")
    logger.info(f"Tổng beer_servings: {df['beer_servings'].sum()}")
    logger.info(f"Tổng spirit_servings: {df['spirit_servings'].sum()}")
    logger.info(f"Tổng wine_servings: {df['wine_servings'].sum()}")
    
    return {
        "status": "success",
        "total_records": len(df),
        "output_file": output_file,
        "file_size_kb": round(file_size_kb, 2),
        "countries_count": df['country'].nunique()
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
    dag_id='extract_alcohol_data',
    default_args=default_args,
    description='Crawl Alcohol Consumption data from FiveThirtyEight GitHub and save to Data Lake',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['extract', 'external', 'alcohol', 'data_lake'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_alcohol_data',
        python_callable=extract_alcohol_data,
        provide_context=True,
    )
    
    extract_task

