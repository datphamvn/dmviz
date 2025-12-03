# 📚 DMVIZ - Tài Liệu Hệ Thống & Khuyến Nghị

## 📋 Mục Lục

1. [Tổng Quan Hệ Thống](#1-tổng-quan-hệ-thống)
2. [Kiến Trúc Hệ Thống](#2-kiến-trúc-hệ-thống)
3. [Data Pipeline & ETL](#3-data-pipeline--etl)
4. [Data Warehouse Schema](#4-data-warehouse-schema)
5. [Machine Learning Models](#5-machine-learning-models)
6. [Core Framework (dmviz)](#6-core-framework-dmviz)
7. [Infrastructure & Deployment](#7-infrastructure--deployment)
8. [Phân Tích RAG & AI Integration](#8-phân-tích-rag--ai-integration)
9. [Recommendations & Roadmap](#9-recommendations--roadmap)

---

## 1. Tổng Quan Hệ Thống

### 1.1 Giới Thiệu

**DMVIZ** (Data Management & Visualization) là một nền tảng dữ liệu đầu-cuối được thiết kế để:

- **Thu thập dữ liệu**: Lấy dữ liệu từ nhiều nguồn (API, tệp, streaming)
- **Xử lý**: Làm sạch và tạo đặc trưng (feature engineering)
- **Lưu trữ**: Lưu trữ theo kiến trúc Data Lake + Data Warehouse
- **Phân tích**: RFM, CLV, Propensity, Uplift Modeling
- **Trực quan hóa**: Báo cáo và biểu đồ

### 1.2 Bối cảnh nghiệp vụ

Dự án tập trung vào **Phân tích Giá trị Khách hàng** cho một nhà bán lẻ trực tuyến (bộ dữ liệu Online Retail II):

| Use Case | Mô Tả |
|----------|-------|
| **Phân đoạn RFM** | Phân khúc khách hàng theo Recency, Frequency, Monetary |
| **Dự đoán CLV** | Dự đoán Customer Lifetime Value cho 12 tháng |
| **Propensity Scoring** | Xác suất mua hàng trong 30 ngày |
| **Uplift Modeling** | Đánh giá hiệu quả chương trình khuyến mãi |
| **Tích hợp macro** | Tích hợp dữ liệu ONS Consumer Trends |

### 1.3 Ngăn xếp công nghệ (Tech Stack)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        NGĂN XẾP CÔNG NGHỆ                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Orchestration    │  Apache Airflow 2.9.3                           │
│  Streaming        │  Apache Kafka 7.5.0 (Confluent)                 │
│  Data Storage     │  HDFS (WebHDFS), PostgreSQL 13                  │
│  Processing       │  Python (Pandas, NumPy), PySpark                │
│  ML/Statistics    │  scikit-learn, lifetimes (BG/NBD, Gamma-Gamma)  │
│  API              │  FastAPI, SQLAlchemy                            │
│  Containerization │  Docker, Docker Compose                         │
│  Visualization    │  Matplotlib (dự kiến)                           │
│  Monitoring       │  Kafka UI, PgAdmin                              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Kiến trúc hệ thống

### 2.1 Kiến trúc tổng quan (High-Level Architecture)

```
                  ┌─────────────────────────────────────────┐
                  │           NGUỒN DỮ LIỆU                  │
                  ├─────────────────────────────────────────┤
                  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  │
                  │  │ Mock API│  │ Dữ liệu  │  │  Tệp    │  │
                  │  │(Nội bộ) │  │ONS (Ngoại)│ │(Excel)  │  │
                  │  └────┬────┘  └────┬────┘  └────┬────┘  │
                  └───────┼───────────┼───────────┼────────┘
                      │           │           │
                      ▼           ▼           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           LỚP INGESTION (Thu thập)                       │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    Các DAG của Apache Airflow                         ││
│  │  ┌───────────────┐  ┌───────────────────┐  ┌──────────────────┐    ││
│  │  │stream_daily_  │  │extract_consumer_  │  │extract_historical│    ││
│  │  │batch          │  │trends             │  │_data             │    ││
│  │  └───────────────┘  └───────────────────┘  └──────────────────┘    ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         HỒ DỮ LIỆU (DATA LAKE - HDFS)                    │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                  │
│  │  KHU RAW    │───▶│  STAGING    │───▶│  CURATED    │                  │
│  │  (Bronze)   │    │ (Silver)    │    │  (Gold)     │                  │
│  └─────────────┘    └─────────────┘    └─────────────┘                  │
│   • internal_streaming  • Dữ liệu đã làm sạch  • Dữ liệu tổng hợp        │
│   • internal_historical  • Feature engineering    • Business-ready       │
│   • external_ons_consumer_trends                                             │
└─────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        LỚP XỬ LÝ (PROCESSING)                            │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                DAG `historical_data_etl_processing`                  │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐               ││
│  │  │ Làm    │─▶│Tạo     │─▶│ Tính    │─▶│ Hồ sơ   │               ││
│  │  │ sạch   │  │đặc trưng│  │ RFM     │  │ phân    │               ││
│  │  │ dữ liệu│  │(Feature)|  │ scoring │  │ đoạn    │               ││
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘               ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    KHO DỮ LIỆU (DATA WAREHOUSE - PostgreSQL)            │
│                                                                          │
│                         ┌──────────────────┐                            │
│                         │   Fact_Sales     │                            │
│                         │  (Giao dịch)     │                            │
│                         └────────┬─────────┘                            │
│                                  │                                       │
│      ┌───────────────────────────┼───────────────────────────┐          │
│      │                           │                           │          │
│      ▼                           ▼                           ▼          │
│  ┌──────────────┐      ┌──────────────┐          ┌──────────────┐      │
│  │ Dim_Customer │      │   Dim_Date   │          │ Dim_Product  │      │
│  │  • RFM       │      │  • DateKey   │          │  • StockCode │      │
│  │  • CLV       │      │  • QuarterKey│          │  • Category  │      │
│  │  • Propensity│      └──────┬───────┘          └──────────────┘      │
│  │  • Uplift    │             │                                        │
│  └──────────────┘             ▼                                        │
│                    ┌────────────────────────┐                          │
│                    │  Dim_Macro_Economic    │                          │
│                    │  (Consumer Trends)     │                          │
│                    │  • COICOP Categories   │                          │
│                    │  • Growth Rates        │                          │
│                    └────────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        LỚP ĐÁNH GIÁ MÔ HÌNH (ML SCORING)                 │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                    DAG `model_scoring`                               ││
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐               ││
│  │  │Trích    │─▶│ Điểm    │─▶│ Điểm    │─▶│ Điểm    │               ││
│  │  │đặc trưng │  │CLV      │  │Propensity│ │Uplift   │               ││
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘               ││
│  │                                              │                       ││
│  │                                              ▼                       ││
│  │                                    ┌──────────────────┐             ││
│  │                                    │Cập nhật `dim_customer`│        ││
│  │                                    └──────────────────┘             ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Details

#### 2.2.1 Data Sources

| Source | Type | Description | Update Frequency |
|--------|------|-------------|------------------|
| **Mock API** | Internal | FastAPI service serving Online Retail II data | Real-time |
| **ONS Consumer Trends** | External | UK Macro-economic data (HHFCE) | Quarterly |
| **Excel Files** | Historical | Online Retail II dataset | One-time load |

#### 2.2.2 Data Partitioning Strategy

Dữ liệu Online Retail II (2009-12-01 đến 2011-12-09) được chia thành 2 phần:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA PARTITIONING STRATEGY                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Dataset Timeline: 2009-12-01 ──────────────────────────── 2011-12-09   │
│                                                                          │
│  ┌─────────────────────────────────────┐  ┌─────────────────────────┐   │
│  │        HISTORICAL DATA              │  │    STREAMING DATA       │   │
│  │    (extract_historical_data)        │  │  (stream_daily_batch)   │   │
│  │                                     │  │                         │   │
│  │    2009-12-01 → 2011-11-30          │  │  2011-12-01 → 2011-12-09│   │
│  │    (~24 months)                     │  │  (9 days)               │   │
│  │                                     │  │                         │   │
│  │    → internal_historical/           │  │  → internal_streaming/  │   │
│  │    → Partitioned by year/month      │  │  → Partitioned by day   │   │
│  │    → One-time manual load           │  │  → Daily simulation     │   │
│  └─────────────────────────────────────┘  └─────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

| Data Type | Date Range | DAG | HDFS Path |
|-----------|------------|-----|-----------|
| **Historical** | 2009-12-01 → 2011-11-30 | `extract_historical_data` | `/data_lake/raw_zone/internal_historical/` |
| **Streaming** | 2011-12-01 → 2011-12-09 | `stream_daily_batch` | `/data_lake/raw_zone/internal_streaming/` |

#### 2.2.3 Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `extract_historical_data` | Manual | Extract historical data (2009-12-01 → 2011-11-30) to HDFS |
| `stream_daily_batch` | Daily | Simulate streaming cho tháng cuối (2011-12-01 → 2011-12-09) |
| `kafka_consumer_to_hdfs` | */30 * * * * | Consume từ Kafka → HDFS (micro-batch) |
| `stream_daily_processing` | Triggered | Validate, Clean, Transform, Aggregate |
| `extract_consumer_trends` | Quarterly | ONS Consumer Trends ETL |
| `historical_data_etl_processing` | Daily | Data cleaning, feature engineering, RFM |
| `model_scoring` | Daily | CLV, Propensity, Uplift scoring |
| `refresh_materialized_views` | Daily | Update analytical views |
| `migration_add_datekey` | Manual | Schema migrations |

---

## 3. Data Pipeline & ETL

### 3.1 Ingestion Pipelines

#### 3.1.1 Kafka Streaming Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         KAFKA STREAMING PIPELINE                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐             │
│  │  Mock API    │────▶│    Kafka     │────▶│    HDFS      │             │
│  │  (Producer)  │     │   Broker     │     │  raw_zone    │             │
│  └──────────────┘     └──────────────┘     └──────┬───────┘             │
│                                                    │                     │
│        ┌──────────────────────────────────────────┘                     │
│        ▼                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐             │
│  │  Validate    │────▶│    Clean     │────▶│  Aggregate   │             │
│  │  Quality     │     │  Transform   │     │  (Curated)   │             │
│  └──────────────┘     └──────────────┘     └──────────────┘             │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

**Kafka Topics:**

| Topic | Partitions | Retention | Purpose |
|-------|------------|-----------|--------|
| `retail.transactions.raw` | 3 | 7 days | Raw transaction events |
| `retail.transactions.validated` | 3 | 7 days | Validated records (future) |
| `retail.transactions.dlq` | 1 | 7 days | Dead Letter Queue |

#### 3.1.2 Kafka Producer (`kafka_producer/producer.py`)

```python
# Pipeline Flow
Mock API → Kafka Producer → retail.transactions.raw topic

# Key Features:
- Paginated API consumption (100 records/batch)
- Message enrichment with metadata (message_id, timestamp, schema_version)
- Partition key by CustomerID (ordering guarantee per customer)
- Gzip compression
- Configurable simulation speed (10x for demo)
- Auto-retry on failures
```

#### 3.1.3 Kafka Consumer DAG (`kafka_consumer_to_hdfs.py`)

```python
# Pipeline Flow
Kafka Topic → Consumer → Group by Date → Parquet → HDFS Upload

# Key Features:
- Micro-batch consumption (every 30 minutes)
- Manual offset commit (after successful HDFS write)
- Message grouping by InvoiceDate
- Hive-style partitioning: year=YYYY/month=MM/day=DD/
- Max batch size: 10,000 messages
- Auto-trigger stream_daily_processing DAG
```

#### 3.1.4 Daily Streaming Batch (`stream_daily_batch.py`)

```python
# Pipeline Flow
API Call (paginated) → DataFrame → Parquet → WebHDFS Upload

# Data Partitioning Strategy:
# - Historical data: extract_historical_data extracts 2009-12-01 → 2011-11-30
# - Streaming simulation: stream_daily_batch simulates only the last month (2011-12-01 → 2011-12-09)

# Date Mapping Logic:
# - DAG start_date: 2025-01-01 → maps to 2011-12-01
# - DAG execution 2025-01-02 → maps to 2011-12-02
# - ... continues for 9 days, then cycles back

# Key Features:
- Paginated API consumption (100 records/batch)
- Date filtering (start_date, end_date)
- Bearer token authentication
- WebHDFS upload with DataNode redirect handling
- Partition by date: /data_lake/raw_zone/internal_streaming/year=YYYY/month=MM/day=DD/
- Auto-trigger stream_daily_processing DAG
```

#### 3.1.2 Consumer Trends ETL (`extract_consumer_trends.py`)

```python
# Pipeline Flow
ONS Website → Download CSV → Parse COICOP Categories → Calculate Growth Rates 
→ Save to HDFS → Load to PostgreSQL (Dim_Macro_Economic)

# Key Features:
- 12 COICOP spending categories
- QoQ and YoY growth rate calculations
- Lagged values for regression models
- Inflation/deflator integration
```

### 3.2 Processing Pipeline (`historical_data_etl_processing.py`)

#### Phase 1: Data Cleaning

```python
# Cleaning Rules:
1. Remove records with missing CustomerID
2. Remove invalid prices (≤ 0)
3. Handle duplicates
4. Separate returns/cancellations (Invoice starts with 'C')
5. Date range validation
```

#### Phase 2: Feature Engineering

```python
# Customer-Level Features:
- TotalSpend: Sum of (Quantity × Price)
- Frequency: Count of unique invoices
- Recency: Days since last purchase
- CustomerLifetime: Days since first purchase
- AvgTransactionValue: TotalSpend / Frequency
- StdTransactionValue: Std deviation of transactions
- AvgItemsPerInvoice: Avg items per order
- PurchaseFrequencyRate: Orders per month
- Tenure: Months as customer
```

#### Phase 3: RFM Scoring

```python
# Scoring Method: Quantile-based (1-5)
R_Score: Recency (lower = better) → Inverse scoring
F_Score: Frequency (higher = better)
M_Score: Monetary (higher = better)

# Segments:
Champions, Loyal Customers, Potential Loyalists, New Customers,
Promising, Need Attention, About to Sleep, At Risk, Can't Lose Them,
Hibernating, Lost
```

### 3.3 Data Quality Checks

```python
# Validation Rules:
1. Row count validation (min threshold)
2. Schema validation (required columns)
3. Data type validation
4. Referential integrity (FK constraints)
5. Business rule validation (e.g., positive amounts)
```

---

## 4. Data Warehouse Schema

### 4.1 Star Schema Design

```sql
                         ┌────────────────────┐
                         │    Fact_Sales      │
                         │   (Transactions)   │
                         └─────────┬──────────┘
                                   │
    ┌──────────────────────────────┼──────────────────────────────┐
    │                              │                              │
    ▼                              ▼                              ▼
┌──────────────┐          ┌──────────────┐              ┌──────────────┐
│ Dim_Customer │          │   Dim_Date   │              │ Dim_Product  │
│              │          │              │              │              │
└──────────────┘          └──────┬───────┘              └──────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Dim_Macro_Economic    │
                    │   (Consumer Trends)    │
                    └────────────────────────┘
```

### 4.2 Table Definitions

#### Fact_Sales

```sql
CREATE TABLE fact_sales (
    TransactionKey      SERIAL PRIMARY KEY,
    Invoice            VARCHAR(20),
    StockCode          VARCHAR(20),
    Description        TEXT,
    Quantity           INT,
    InvoiceDate        TIMESTAMP,
    Price              DECIMAL(10,2),
    CustomerID         INT,
    Country            VARCHAR(50),
    TotalAmount        DECIMAL(12,2),
    DateKey            INT,           -- FK to Dim_Date
    CustomerKey        INT,           -- FK to Dim_Customer
    ProductKey         INT            -- FK to Dim_Product
);
```

#### Dim_Customer

```sql
CREATE TABLE dim_customer (
    CustomerKey         SERIAL PRIMARY KEY,
    CustomerID          INT UNIQUE,
    Country             VARCHAR(50),
    
    -- RFM Metrics
    Recency             INT,
    Frequency           INT,
    Monetary            DECIMAL(12,2),
    R_Score             INT,
    F_Score             INT,
    M_Score             INT,
    RFM_Score           VARCHAR(3),
    RFM_Score_Numeric   DECIMAL(5,2),
    RFM_Segment         VARCHAR(50),
    
    -- Behavioral Features
    CustomerLifetime    INT,
    AvgTransactionValue DECIMAL(10,2),
    StdTransactionValue DECIMAL(10,2),
    AvgItemsPerInvoice  DECIMAL(10,2),
    PurchaseFrequencyRate DECIMAL(10,4),
    Tenure              INT,
    
    -- ML Scores (updated by model_scoring DAG)
    CLV_12M_Predicted   DECIMAL(12,2),
    Propensity_Score    DECIMAL(10,6),
    Uplift_Score        DECIMAL(10,6),
    
    -- Metadata
    ETL_Loaded_At       TIMESTAMP,
    ETL_Updated_At      TIMESTAMP
);
```

#### Dim_Macro_Economic

```sql
CREATE TABLE dim_macro_economic (
    QuarterKey          VARCHAR(7) PRIMARY KEY,  -- YYYY-Q#
    Year                INT,
    Quarter             INT,
    
    -- Aggregate HHFCE
    HFC_Total_CVM_SA    DECIMAL(18,2),
    HFC_Total_Growth_QoQ DECIMAL(10,6),
    HFC_Total_Growth_YoY DECIMAL(10,6),
    
    -- COICOP Categories (12 categories)
    COICOP_01_Food_NAB        DECIMAL(18,2),
    COICOP_02_Alcohol_Tobacco DECIMAL(18,2),
    COICOP_03_Clothing        DECIMAL(18,2),
    -- ... (continuing for all 12 categories)
    
    -- Lagged Values
    HFC_Lagged_1Q             DECIMAL(18,2),
    HFC_Growth_QoQ_Lagged     DECIMAL(10,6),
    
    -- Inflation
    Inflation_Rate_QoQ        DECIMAL(10,6),
    Inflation_Rate_YoY        DECIMAL(10,6)
);
```

### 4.3 Materialized Views

```sql
-- Customer Quarterly Analysis (pre-aggregated for performance)
CREATE MATERIALIZED VIEW mv_customer_quarterly_analysis AS
SELECT 
    dc.customerkey,
    dc.rfm_segment,
    dd.quarterkey,
    SUM(fs.totalamount) AS quarterly_spend,
    COUNT(DISTINCT fs.invoice) AS quarterly_orders,
    me.hfc_total_cvm_sa AS national_hfc_total,
    me.hfc_total_growth_qoq AS national_hfc_growth
FROM fact_sales fs
JOIN dim_customer dc ON fs.customerkey = dc.customerkey
JOIN dim_date dd ON fs.datekey = dd.datekey
JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
GROUP BY dc.customerkey, dc.rfm_segment, dd.quarterkey, 
         me.hfc_total_cvm_sa, me.hfc_total_growth_qoq;
```

---

## 5. Machine Learning Models

### 5.1 Model Overview

| Model | Purpose | Algorithm | Output |
|-------|---------|-----------|--------|
| **CLV** | 12-month Customer Lifetime Value | BG/NBD + Gamma-Gamma | `CLV_12M_Predicted` |
| **Propensity** | 30-day purchase probability | Logistic Regression | `Propensity_Score` |
| **Uplift** | Promotion response lift | T-Learner + S-Learner Ensemble | `Uplift_Score` |

### 5.2 CLV Model (BG/NBD + Gamma-Gamma)

```python
# Model Components:
1. BG/NBD (Beta Geometric/Negative Binomial Distribution)
   - Predicts number of future transactions
   - Calculates probability customer is "alive"
   
2. Gamma-Gamma
   - Predicts expected average transaction value
   
# CLV Formula:
CLV = predicted_purchases × predicted_avg_value × prob_alive × discount_factor

# Parameters:
- Calibration Period: 75% of data
- Prediction Period: 12 months
- Discount Rate: 1%/month (12%/year)

# Fallback (when BG/NBD fails):
CLV = avg_monthly_spend × months × retention_prob × discount_factor
- retention_prob based on RFM segment mapping
```

### 5.3 Propensity Model

```python
# Algorithm: Logistic Regression with L2 regularization

# Features:
- RFM scores (r_score, f_score, m_score)
- Behavioral metrics (recency, frequency, monetary)
- Lifecycle features (customerlifetime, tenure)
- Purchase patterns (purchasefrequencyrate, avgtransactionvalue)

# Target: will_purchase_30d (proxy: recency < 60)

# Output: Probability [0.01, 0.99]
```

### 5.4 Uplift Model (T-Learner + S-Learner)

```python
# Approach: Ensemble of two meta-learners

# T-Learner:
- Train separate models for treatment and control groups
- Uplift = P(Y=1|T=1, X) - P(Y=1|T=0, X)

# S-Learner:
- Single model with treatment as feature
- Uplift = model(X, T=1) - model(X, T=0)

# Ensemble: Average of T-Learner and S-Learner predictions

# Customer Categories:
| Category      | Propensity | Uplift | Action              |
|---------------|------------|--------|---------------------|
| Persuadables  | High       | High   | Target with promo   |
| Sure Things   | High       | Low    | No promo needed     |
| Lost Causes   | Low        | Low    | Don't target        |
| Do Not Disturb| Low        | Negative| Avoid promotion    |
```

---

## 6. Core Framework (dmviz)

### 6.1 Architecture

```
dmviz/
├── src/
│   ├── core/
│   │   ├── base.py         # BaseComponent, Pipeline, StageResult
│   │   └── registry.py     # Component registry
│   ├── ingestion/
│   │   ├── base.py         # BaseIngester, BatchIngester
│   │   ├── connectors.py   # CSV, JSON, API, Parquet ingesters
│   │   └── streaming.py    # KafkaConsumer
│   ├── processing/
│   │   ├── base.py         # BaseProcessor, ProcessorChain
│   │   ├── cleaners.py     # MissingHandler, OutlierHandler
│   │   ├── transformers.py # Normalizer, Encoder
│   │   └── features.py     # FeatureEngineer, ColumnSelector
│   ├── storage/
│   │   ├── base.py         # BaseStorage, DataLake
│   │   ├── file.py         # ParquetStorage, CSVStorage
│   │   └── warehouse.py    # WarehouseStorage
│   ├── models/
│   │   └── base.py         # BaseModel, ModelResult
│   ├── viz/
│   │   └── base.py         # Figure, Plotter
│   └── utils/
│       └── helpers.py
└── datasets/
    ├── __init__.py
    └── catalog.yaml
```

### 6.2 Core Contracts

```python
# Pipeline Flow:
[Datasets/Ingestion] → [Processing] → [Storage] → [Models] → [Viz]
   name | str|Path      DataFrame     DataFrame   arrays     arrays
         ↓                 ↓              ↓          ↓          ↓
      DataFrame         DataFrame       bool      arrays     Figure

# BaseComponent Contract:
class BaseComponent(ABC, Generic[InputT, OutputT]):
    def execute(input_data: InputT) -> StageResult[OutputT]
    def _run(input_data: InputT) -> OutputT  # Abstract

# StageResult:
@dataclass
class StageResult:
    data: T
    success: bool
    stage: str
    timestamp: datetime
    metrics: dict
    errors: list[str]
```

### 6.3 Current Implementation Status

| Module | Status | Notes |
|--------|--------|-------|
| `core/base.py` | ⚠️ Partial | Pipeline.run() not implemented |
| `core/registry.py` | ✅ Complete | Registration & retrieval working |
| `ingestion/*` | ⚠️ Skeleton | All _run() methods raise NotImplementedError |
| `processing/*` | ⚠️ Skeleton | All _run() methods raise NotImplementedError |
| `storage/*` | ⚠️ Skeleton | All _run() methods raise NotImplementedError |
| `models/base.py` | ⚠️ Skeleton | Abstract base only |
| `viz/base.py` | ⚠️ Skeleton | Abstract base only |

---

## 7. Infrastructure & Deployment

### 7.1 Docker Services

```yaml
# docker-compose-anhth.yml

services:
  # ============== CORE SERVICES ==============
  mock_api:           # FastAPI service for transaction data
    port: 1000
    
  postgres:           # PostgreSQL 13 for Airflow metadata + DW
    port: 5432
    
  pgadmin:            # Database administration UI
    port: 5050
    
  # ============== AIRFLOW ==============
  airflow-webserver:  # Airflow UI
    port: 8080
    
  airflow-scheduler:  # DAG scheduling
    
  airflow-init:       # Initialization
  
  # ============== KAFKA CLUSTER ==============
  zookeeper:          # Kafka coordination
    port: 2181
    image: confluentinc/cp-zookeeper:7.5.0
    
  kafka:              # Message broker
    ports: 9092, 29092
    image: confluentinc/cp-kafka:7.5.0
    
  kafka-ui:           # Kafka monitoring UI
    port: 8081
    image: provectuslabs/kafka-ui:latest
    
  kafka-init:         # Topic initialization
    # Creates: retail.transactions.raw, .validated, .dlq
    
  kafka-producer:     # Streaming producer (profile: streaming)
    # Reads from API, publishes to Kafka
```

### 7.2 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Kafka UI | http://localhost:8081 | - |
| PgAdmin | http://localhost:5050 | admin@pgadmin.com / admin |
| Mock API Docs | http://localhost:1000/docs | Bearer token |

### 7.3 Environment Configuration

```bash
# Key Environment Variables:

# API Configuration
API_BASE_URL=http://data_gov_api:80/api/v1/raw_transactions
API_KEY=<your_api_key>

# HDFS Configuration
WEBHDFS_HOST=192.168.1.6
WEBHDFS_PORT=9870
HDFS_USER=anhth

# PostgreSQL Configuration
POSTGRES_HOST=192.168.1.6
POSTGRES_PORT=5432
POSTGRES_DB=retail_dw
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC_RAW=retail.transactions.raw
KAFKA_CONSUMER_GROUP=airflow-hdfs-consumer
```

### 7.4 HDFS Structure

```
/data_lake/
├── raw_zone/
│   ├── internal_streaming/              # Streaming simulation (Dec 2011)
│   │   └── year=2011/
│   │       └── month=12/
│   │           └── day=DD/              # DD: 01-09
│   │               ├── kafka_batch_*.parquet
│   │               └── transactions_*.parquet
│   ├── internal_historical/             # Historical data (Dec 2009 - Nov 2011)
│   │   └── year=YYYY/
│   │       └── month=MM/
│   │           └── transactions_YYYY-MM.parquet
│   └── external_ons_consumer_trends/
│       └── consumer_trends_YYYY-Q#.parquet
├── staging_zone/
│   └── transactions/
│       └── cleaned_transactions_YYYYMMDD.parquet
└── curated_zone/
    ├── daily_stats/
    │   └── daily_stats_YYYYMMDD.parquet
    ├── dim_customer.parquet
    ├── dim_product.parquet
    └── fact_sales.parquet
```
---

## 8. Streaming Processing Pipeline

### 8.1 Stream Daily Processing DAG (`stream_daily_processing.py`)

DAG này được trigger tự động bởi `kafka_consumer_to_hdfs` hoặc `stream_daily_batch`.

```python
# Pipeline Flow
Extract (HDFS) → Validate Quality → Clean & Transform → Aggregate → Curated Zone

# Tasks:
1. extract_raw_data      # Đọc parquet files từ HDFS raw_zone
2. check_data_exists     # Branch: có data hay không
3. validate_quality      # Kiểm tra data quality (score > 60 to pass)
4. transform_and_clean   # Làm sạch, chuẩn hóa, thêm metadata
5. compute_daily_aggregates  # Tính toán KPIs hàng ngày
```

### 8.2 Data Quality Validation

```python
# Quality Checks:
- Schema validation (required columns exist)
- Null rate check (CustomerID null < 25%)
- Invalid price detection (Price <= 0)
- Zero quantity detection
- Quality score calculation (100 - penalties)

# Quality Score Thresholds:
- Score >= 60: PASS → Continue processing
- Score < 60: FAIL → Alert and stop
```

### 8.3 Data Cleaning Rules

```python
# Cleaning Steps:
1. Remove null CustomerID
2. Convert CustomerID to int
3. Remove invalid prices (Price <= 0)
4. Parse InvoiceDate to datetime
5. Add is_return flag (Invoice starts with 'C')
6. Calculate TotalAmount = Quantity × Price
7. Add metadata (_processed_at, _source)
```

### 8.4 Daily Aggregates Output

```python
# Computed Metrics:
{
    "date": "2009-12-01",
    "total_revenue": 15420.50,
    "total_orders": 145,
    "total_customers": 89,
    "total_items_sold": 3250,
    "avg_order_value": 106.35,
    "unique_products": 412,
    "top_countries": {"United Kingdom": 120, ...},
    "returns_count": 12,
    "returns_amount": 450.00
}
```

---

## 9. Quick Start Guide

### 9.1 Khởi động hệ thống

```bash
# 1. Start all services
cd /home/anhth/project/dmviz/docker
sudo docker compose -f docker-compose-anhth.yml up -d

# 2. Wait for services to be ready (30-60 seconds)
sleep 30

# 3. Verify Kafka topics
sudo docker exec data_gov_kafka kafka-topics --bootstrap-server localhost:9092 --list

# 4. Start Kafka producer (optional - for streaming simulation)
sudo docker compose -f docker-compose-anhth.yml --profile streaming up -d kafka-producer
```

### 9.2 Monitor & Debug

```bash
# View Kafka producer logs
sudo docker logs -f data_gov_kafka_producer

# Check messages in topic
sudo docker exec data_gov_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic retail.transactions.raw \
  --from-beginning --max-messages 5

# Trigger consumer DAG manually
sudo docker exec docker-airflow-scheduler-1 \
  airflow dags trigger kafka_consumer_to_hdfs

# Check DAG status
sudo docker exec docker-airflow-scheduler-1 \
  airflow dags list-runs -d kafka_consumer_to_hdfs
```

### 9.3 Verify Data in HDFS

```bash
# List partitions
curl -s "http://192.168.1.6:9870/webhdfs/v1/data_lake/raw_zone/internal_streaming?op=LISTSTATUS&user.name=anhth"

# Check specific date partition
curl -s "http://192.168.1.6:9870/webhdfs/v1/data_lake/raw_zone/internal_streaming/year=2009/month=12/day=01?op=LISTSTATUS&user.name=anhth"
```

---

*Document Version: 1.1*
*Last Updated: December 2025*
*Author: Data Engineering Team*
*Changes: Added Kafka Streaming Architecture (Section 3.1, 7, 8, 9)*
