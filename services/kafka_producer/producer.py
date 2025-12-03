"""
Kafka Producer Service
Simulates real-time streaming by reading from Mock API and publishing to Kafka
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import hashlib

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC_RAW = os.environ.get('KAFKA_TOPIC_RAW', 'retail.transactions.raw')
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://data_gov_api:80/api/v1/raw_transactions')
API_KEY = os.environ.get('API_KEY', 'mysecretkey')

# Streaming simulation config
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '100'))
DELAY_BETWEEN_BATCHES = float(os.environ.get('DELAY_BETWEEN_BATCHES', '1.0'))  # seconds
SIMULATION_SPEED = float(os.environ.get('SIMULATION_SPEED', '1.0'))  # 1.0 = real-time, 10.0 = 10x faster

# Date range for simulation (Online Retail II dataset)
START_DATE = datetime(2009, 12, 1)
END_DATE = datetime(2011, 12, 9)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_kafka_producer() -> KafkaProducer:
    """Create and configure Kafka producer with retries"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip',
                batch_size=16384,
                linger_ms=10,
            )
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise


def get_partition_key(record: dict) -> str:
    """
    Generate partition key based on CustomerID for ordering guarantee per customer
    Falls back to Invoice if CustomerID is null
    """
    customer_id = record.get('CustomerID')
    if customer_id and customer_id != 'null':
        return f"customer_{int(float(customer_id))}"
    return f"invoice_{record.get('Invoice', 'unknown')}"


def fetch_transactions_from_api(start_date: datetime, end_date: datetime, 
                                 limit: int = 100, offset: int = 0) -> list:
    """Fetch transactions from Mock API with date filtering"""
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    
    params = {
        "limit": limit,
        "offset": offset,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    
    try:
        response = requests.get(API_BASE_URL, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching from API: {e}")
        return []


def enrich_message(record: dict, sequence_num: int) -> dict:
    """
    Enrich transaction record with metadata for streaming
    """
    # Generate unique message ID
    message_content = f"{record.get('Invoice')}_{record.get('StockCode')}_{record.get('InvoiceDate')}_{sequence_num}"
    message_id = hashlib.md5(message_content.encode()).hexdigest()
    
    return {
        "metadata": {
            "message_id": message_id,
            "source": "mock_api",
            "ingestion_timestamp": datetime.utcnow().isoformat(),
            "sequence_number": sequence_num,
            "schema_version": "1.0"
        },
        "data": record
    }


def on_send_success(record_metadata):
    """Callback for successful message delivery"""
    logger.debug(f"Message delivered to {record_metadata.topic}:{record_metadata.partition} "
                f"offset={record_metadata.offset}")


def on_send_error(exc):
    """Callback for failed message delivery"""
    logger.error(f"Failed to deliver message: {exc}")


def stream_daily_transactions(producer: KafkaProducer, target_date: datetime) -> dict:
    """
    Stream all transactions for a specific date to Kafka
    Returns statistics about the streaming session
    """
    logger.info(f"Starting streaming for date: {target_date.strftime('%Y-%m-%d')}")
    
    stats = {
        "date": target_date.strftime("%Y-%m-%d"),
        "total_sent": 0,
        "total_failed": 0,
        "start_time": datetime.utcnow().isoformat()
    }
    
    offset = 0
    sequence_num = 0
    
    while True:
        # Fetch batch from API
        records = fetch_transactions_from_api(
            start_date=target_date,
            end_date=target_date,
            limit=BATCH_SIZE,
            offset=offset
        )
        
        if not records:
            break
        
        # Send each record to Kafka
        for record in records:
            try:
                partition_key = get_partition_key(record)
                enriched_message = enrich_message(record, sequence_num)
                
                # Async send with callbacks
                future = producer.send(
                    KAFKA_TOPIC_RAW,
                    key=partition_key,
                    value=enriched_message
                )
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)
                
                stats["total_sent"] += 1
                sequence_num += 1
                
            except Exception as e:
                logger.error(f"Error sending message: {e}")
                stats["total_failed"] += 1
        
        logger.info(f"Sent batch: offset={offset}, records={len(records)}")
        
        # Check if we've reached the end
        if len(records) < BATCH_SIZE:
            break
        
        offset += BATCH_SIZE
        
        # Delay between batches to simulate real-time
        if DELAY_BETWEEN_BATCHES > 0:
            time.sleep(DELAY_BETWEEN_BATCHES / SIMULATION_SPEED)
    
    # Ensure all messages are sent
    producer.flush()
    
    stats["end_time"] = datetime.utcnow().isoformat()
    logger.info(f"Completed streaming for {target_date.strftime('%Y-%m-%d')}: "
               f"sent={stats['total_sent']}, failed={stats['total_failed']}")
    
    return stats


def run_continuous_simulation(producer: KafkaProducer):
    """
    Run continuous simulation, streaming data day by day
    """
    current_date = START_DATE
    
    while current_date <= END_DATE:
        stats = stream_daily_transactions(producer, current_date)
        
        if stats["total_sent"] > 0:
            logger.info(f"Daily summary: {stats}")
        
        current_date += timedelta(days=1)
        
        # Delay between days (simulate daily batches)
        day_delay = 5.0 / SIMULATION_SPEED  # 5 seconds between days by default
        logger.info(f"Waiting {day_delay:.1f}s before next day...")
        time.sleep(day_delay)
    
    logger.info("Simulation completed - reached end of dataset")


def run_single_day(producer: KafkaProducer, date_str: str):
    """
    Stream a single day's transactions
    """
    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    stats = stream_daily_transactions(producer, target_date)
    return stats


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("Kafka Producer Service Starting")
    logger.info(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC_RAW}")
    logger.info(f"API URL: {API_BASE_URL}")
    logger.info("=" * 60)
    
    # Wait for dependencies
    logger.info("Waiting for services to be ready...")
    time.sleep(10)
    
    # Create producer
    producer = create_kafka_producer()
    
    try:
        # Check for specific date from environment
        single_date = os.environ.get('STREAM_DATE')
        
        if single_date:
            logger.info(f"Running single day mode for: {single_date}")
            stats = run_single_day(producer, single_date)
            print(json.dumps(stats, indent=2))
        else:
            logger.info("Running continuous simulation mode")
            run_continuous_simulation(producer)
            
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    main()
