# DMViz Docker Services

This Docker Compose setup provides a complete data engineering stack with the following services:

## Services Overview

| Service | Port | Description | UI URL |
|---------|------|-------------|--------|
| **PostgreSQL** | 5432 | Relational database | - |
| **Zookeeper** | 2181 | Kafka coordination | - |
| **Kafka** | 9092, 29092 | Message streaming | - |
| **Kafka UI** | 8080 | Kafka management interface | http://localhost:8080 |
| **HDFS NameNode** | 9870, 9000 | Distributed file system | http://localhost:9870 |
| **HDFS DataNode** | - | HDFS data storage | - |
| **Spark Master** | 7077, 8081 | Distributed computing | http://localhost:8081 |
| **Spark Worker** | - | Spark execution node | - |
| **Airflow Webserver** | 8082 | Workflow orchestration | http://localhost:8082 |
| **Airflow Scheduler** | - | DAG scheduling | - |
| **Superset** | 8088 | Data visualization | http://localhost:8088 |
| **Redis** | 6379 | Caching layer | - |

## Quick Start

### 1. Start all services

```bash
cd dmviz/docker
docker-compose up -d
```

### 2. Wait for services to initialize

First startup may take a few minutes as images are downloaded and databases are initialized.

```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 3. Access the services

- **Kafka UI**: http://localhost:8080
- **HDFS**: http://localhost:9870
- **Spark**: http://localhost:8081
- **Airflow**: http://localhost:8082 (admin/admin)
- **Superset**: http://localhost:8088 (admin/admin)

## Default Credentials

| Service | Username | Password |
|---------|----------|----------|
| PostgreSQL | dmviz | dmviz123 |
| Airflow | admin | admin |
| Superset | admin | admin |

## Service Commands

### Start specific services

```bash
# Start only PostgreSQL and Kafka
docker-compose up -d postgres zookeeper kafka

# Start the data processing stack
docker-compose up -d postgres kafka spark-master spark-worker
```

### Stop services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

### View logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
docker-compose logs -f airflow-webserver
```

### Scale workers

```bash
# Add more Spark workers
docker-compose up -d --scale spark-worker=3
```

## Service Connections

### From Python (inside dmviz container)

```python
# PostgreSQL
import psycopg2
conn = psycopg2.connect(
    host="postgres",
    database="dmviz",
    user="dmviz",
    password="dmviz123"
)

# Kafka
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:29092'])

# Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DMViz") \
    .getOrCreate()

# HDFS
from hdfs import Client
client = Client('http://namenode:9870')
```

### From host machine

```python
# PostgreSQL
conn = psycopg2.connect(host="localhost", port=5432, ...)

# Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        dmviz-network                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │
│  │ Airflow  │    │  Spark   │    │  Kafka   │    │   HDFS   │ │
│  │ :8082    │    │  :8081   │    │  :9092   │    │  :9870   │ │
│  └────┬─────┘    └────┬─────┘    └────┬─────┘    └────┬─────┘ │
│       │               │               │               │        │
│       └───────────────┴───────────────┴───────────────┘        │
│                           │                                     │
│                    ┌──────┴──────┐                             │
│                    │  PostgreSQL │                             │
│                    │    :5432    │                             │
│                    └──────┬──────┘                             │
│                           │                                     │
│                    ┌──────┴──────┐                             │
│                    │  Superset   │                             │
│                    │    :8088    │                             │
│                    └─────────────┘                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Troubleshooting

### Services not starting

```bash
# Check for errors
docker-compose logs <service-name>

# Restart a specific service
docker-compose restart <service-name>
```

### Permission issues with Airflow

```bash
# Set the Airflow UID
echo -e "AIRFLOW_UID=$(id -u)" >> .env
docker-compose up -d
```

### Out of disk space

```bash
# Clean up unused Docker resources
docker system prune -a --volumes
```

### HDFS not accessible

Wait for the namenode to exit safe mode:

```bash
docker exec dmviz-namenode hdfs dfsadmin -safemode leave
```

