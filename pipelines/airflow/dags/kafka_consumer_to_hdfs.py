"""
DAG: Kafka Consumer to HDFS
Consumes messages from Kafka topic và lưu vào HDFS Data Lake
Chạy micro-batch: mỗi giờ hoặc khi có đủ messages
"""

from datetime import datetime, timedelta
import json
import os
import logging
import tempfile
from typing import List, Dict, Any

import pandas as pd
import requests
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC_RAW = os.environ.get('KAFKA_TOPIC_RAW', 'retail.transactions.raw')
KAFKA_CONSUMER_GROUP = os.environ.get('KAFKA_CONSUMER_GROUP', 'airflow-hdfs-consumer')

# WebHDFS Configuration
WEBHDFS_HOST = os.environ.get('WEBHDFS_HOST', '192.168.1.6')
WEBHDFS_PORT = os.environ.get('WEBHDFS_PORT', '9870')
HDFS_USER = os.environ.get('HDFS_USER', 'anhth')
HDFS_OUTPUT_DIR = '/data_lake/raw_zone/internal_streaming'

# Batch settings
MIN_BATCH_SIZE = int(os.environ.get('MIN_BATCH_SIZE', '100'))
MAX_BATCH_SIZE = int(os.environ.get('MAX_BATCH_SIZE', '10000'))
CONSUMER_TIMEOUT_MS = int(os.environ.get('CONSUMER_TIMEOUT_MS', '30000'))  # 30 seconds

logger = logging.getLogger(__name__)


def create_kafka_consumer() -> KafkaConsumer:
    """Create Kafka consumer with auto-commit disabled for manual offset management"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manual commit after successful HDFS write
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
            max_poll_records=500,
        )
        logger.info(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        return consumer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise


def ensure_hdfs_directory(hdfs_dir: str) -> bool:
    """Ensure HDFS directory exists using WebHDFS"""
    try:
        check_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_dir}?op=GETFILESTATUS&user.name={HDFS_USER}"
        response = requests.get(check_url, timeout=30)
        
        if response.status_code == 200:
            return True
        
        mkdir_url = f"http://{WEBHDFS_HOST}:{WEBHDFS_PORT}/webhdfs/v1{hdfs_dir}?op=MKDIRS&user.name={HDFS_USER}"
        response = requests.put(mkdir_url, timeout=30)
        
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Error ensuring directory: {str(e)}")
        return False


def upload_to_webhdfs(local_file: str, hdfs_path: str) -> bool:
    """Upload file to HDFS using WebHDFS REST API"""
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
            
            if upload_response.status_code == 201:
                logger.info(f"Successfully uploaded to HDFS: {hdfs_path}")
                return True
            else:
                logger.error(f"Failed to upload. Status: {upload_response.status_code}")
                return False
        else:
            logger.error(f"Failed to create file. Status: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error uploading to HDFS: {str(e)}")
        return False


def consume_and_save_to_hdfs(**context) -> Dict[str, Any]:
    """
    Main task: Consume messages from Kafka and save to HDFS
    Returns statistics about the consumption
    """
    execution_date = context['execution_date']
    logger.info(f"Starting Kafka consumption at {execution_date}")
    
    stats = {
        "execution_date": execution_date.isoformat(),
        "messages_consumed": 0,
        "messages_failed": 0,
        "files_written": 0,
        "partitions_by_date": {}
    }
    
    consumer = None
    messages_by_date: Dict[str, List[Dict]] = {}
    
    try:
        consumer = create_kafka_consumer()
        
        # Consume messages
        message_count = 0
        for message in consumer:
            try:
                # Extract data from enriched message
                if 'data' in message.value:
                    record = message.value['data']
                    metadata = message.value.get('metadata', {})
                else:
                    record = message.value
                    metadata = {}
                
                # Add kafka metadata
                record['_kafka_partition'] = message.partition
                record['_kafka_offset'] = message.offset
                record['_kafka_timestamp'] = message.timestamp
                record['_ingestion_time'] = metadata.get('ingestion_timestamp', datetime.utcnow().isoformat())
                
                # Group by date
                invoice_date_str = record.get('InvoiceDate', '')
                if invoice_date_str:
                    try:
                        invoice_date = datetime.fromisoformat(invoice_date_str.replace('Z', '+00:00'))
                        date_key = invoice_date.strftime('%Y-%m-%d')
                    except:
                        date_key = execution_date.strftime('%Y-%m-%d')
                else:
                    date_key = execution_date.strftime('%Y-%m-%d')
                
                if date_key not in messages_by_date:
                    messages_by_date[date_key] = []
                messages_by_date[date_key].append(record)
                
                message_count += 1
                stats["messages_consumed"] += 1
                
                # Stop if we've reached max batch size
                if message_count >= MAX_BATCH_SIZE:
                    logger.info(f"Reached max batch size ({MAX_BATCH_SIZE}), stopping consumption")
                    break
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                stats["messages_failed"] += 1
        
        logger.info(f"Consumed {message_count} messages from Kafka")
        
        # Write to HDFS partitioned by date
        if messages_by_date:
            with tempfile.TemporaryDirectory() as temp_dir:
                for date_key, records in messages_by_date.items():
                    if not records:
                        continue
                    
                    df = pd.DataFrame(records)
                    
                    # Parse date for partition path
                    date_parts = date_key.split('-')
                    year, month, day = date_parts[0], date_parts[1], date_parts[2]
                    
                    partition_dir = f"{HDFS_OUTPUT_DIR}/year={year}/month={month}/day={day}"
                    
                    # Generate unique filename with timestamp
                    batch_id = execution_date.strftime('%Y%m%d_%H%M%S')
                    filename = f"kafka_batch_{batch_id}_{date_key.replace('-', '')}.parquet"
                    
                    # Ensure directory exists
                    ensure_hdfs_directory(partition_dir)
                    
                    # Save locally then upload
                    local_file = os.path.join(temp_dir, filename)
                    df.to_parquet(local_file, index=False, engine='pyarrow')
                    
                    hdfs_path = f"{partition_dir}/{filename}"
                    if upload_to_webhdfs(local_file, hdfs_path):
                        stats["files_written"] += 1
                        stats["partitions_by_date"][date_key] = len(records)
                        logger.info(f"Written {len(records)} records to {hdfs_path}")
                    else:
                        logger.error(f"Failed to write {hdfs_path}")
            
            # Commit offsets after successful write
            consumer.commit()
            logger.info("Committed Kafka offsets")
        
    except Exception as e:
        logger.error(f"Error in consume_and_save_to_hdfs: {e}")
        raise
    finally:
        if consumer:
            consumer.close()
            logger.info("Closed Kafka consumer")
    
    # Push stats to XCom
    context['ti'].xcom_push(key='consumption_stats', value=stats)
    
    return stats


def check_messages_available(**context) -> str:
    """
    Branch operator: Check if there are messages in Kafka to consume
    Returns task_id to execute next
    """
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
            group_id=f"{KAFKA_CONSUMER_GROUP}_check",
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
        )
        
        # Get topic partitions
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC_RAW)
        
        if not partitions:
            logger.info("No partitions found for topic")
            consumer.close()
            return 'skip_consumption'
        
        # Check lag for each partition
        topic_partitions = [TopicPartition(KAFKA_TOPIC_RAW, p) for p in partitions]
        consumer.assign(topic_partitions)
        
        # Get end offsets (latest)
        end_offsets = consumer.end_offsets(topic_partitions)
        
        # Get committed offsets (what we've already processed)
        total_lag = 0
        for tp in topic_partitions:
            committed = consumer.committed(tp)
            end = end_offsets[tp]
            if committed is None:
                committed = 0
            lag = end - committed
            total_lag += lag
            logger.info(f"Partition {tp.partition}: committed={committed}, end={end}, lag={lag}")
        
        consumer.close()
        
        if total_lag >= MIN_BATCH_SIZE:
            logger.info(f"Total lag ({total_lag}) >= min batch size ({MIN_BATCH_SIZE}), proceeding with consumption")
            return 'consume_and_save'
        else:
            logger.info(f"Total lag ({total_lag}) < min batch size ({MIN_BATCH_SIZE}), skipping")
            return 'skip_consumption'
            
    except Exception as e:
        logger.error(f"Error checking Kafka messages: {e}")
        return 'skip_consumption'


def log_stats(**context):
    """Log consumption statistics"""
    ti = context['ti']
    stats = ti.xcom_pull(task_ids='consume_and_save', key='consumption_stats')
    
    if stats:
        logger.info("=" * 60)
        logger.info("KAFKA CONSUMPTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Messages consumed: {stats.get('messages_consumed', 0)}")
        logger.info(f"Messages failed: {stats.get('messages_failed', 0)}")
        logger.info(f"Files written: {stats.get('files_written', 0)}")
        logger.info(f"Partitions by date: {stats.get('partitions_by_date', {})}")
        logger.info("=" * 60)
    else:
        logger.info("No consumption statistics available")


# DAG Definition
default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='kafka_consumer_to_hdfs',
    default_args=default_args,
    description='Consume transactions from Kafka and save to HDFS Data Lake',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # Only one instance at a time
    tags=['kafka', 'streaming', 'hdfs', 'ingestion'],
) as dag:
    
    # Check if there are messages to consume
    check_messages = BranchPythonOperator(
        task_id='check_messages',
        python_callable=check_messages_available,
        provide_context=True,
    )
    
    # Skip if not enough messages
    skip_consumption = EmptyOperator(
        task_id='skip_consumption',
    )
    
    # Consume and save to HDFS
    consume_and_save = PythonOperator(
        task_id='consume_and_save',
        python_callable=consume_and_save_to_hdfs,
        provide_context=True,
    )
    
    # Log statistics
    log_consumption_stats = PythonOperator(
        task_id='log_stats',
        python_callable=log_stats,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',
    )
    
    # Trigger processing DAG if we consumed data
    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_processing',
        trigger_dag_id='stream_daily_processing',  # Will create this DAG
        conf={'source': 'kafka', 'execution_date': '{{ ds }}'},
        wait_for_completion=False,
        trigger_rule='none_failed_min_one_success',
    )
    
    # End task for skip branch
    end_skip = EmptyOperator(
        task_id='end_skip',
        trigger_rule='none_failed_min_one_success',
    )
    
    # Task dependencies
    check_messages >> [consume_and_save, skip_consumption]
    consume_and_save >> log_consumption_stats >> trigger_processing
    skip_consumption >> end_skip
