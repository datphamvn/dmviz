# Consumer Trends Data Enrichment Pipeline Design

## 📋 Tổng Quan

Tài liệu này mô tả thiết kế chi tiết các DAGs và quy trình xử lý để **enrich** dữ liệu khách hàng Online Retail II với bộ dữ liệu **Consumer Trends Time Series** từ **Office for National Statistics (ONS)**.

### Mục tiêu
- Tích hợp dữ liệu kinh tế vĩ mô UK (Consumer Trends) vào Data Warehouse
- Hỗ trợ phân tích tác động yếu tố vĩ mô lên hành vi chi tiêu khách hàng
- Tạo nền tảng cho mô hình Panel OLS Fixed Effects với biến HFC_Growth

### Dataset Nguồn
| Dataset | Nguồn | Format | Frequency |
|---------|-------|--------|-----------|
| Consumer Trends | ONS UK | CSV/XLSX | Quarterly |
| URL | https://www.ons.gov.uk/economy/nationalaccounts/satelliteaccounts/datasets/consumertrends | - | - |

---

## 🏗️ Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      CONSUMER TRENDS ENRICHMENT ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │    ONS      │     │   HDFS      │     │   Spark     │     │ PostgreSQL  │   │
│  │  Website    │────▶│  Raw Zone   │────▶│ Processing  │────▶│     DW      │   │
│  │  (CT Data)  │     │             │     │             │     │             │   │
│  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘   │
│         │                   │                   │                   │          │
│         ▼                   ▼                   ▼                   ▼          │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │   DAG 1:    │     │   DAG 2:    │     │   DAG 3:    │     │   DAG 4:    │   │
│  │  Ingestion  │     │  Transform  │     │   Enrich    │     │   Quality   │   │
│  │             │     │             │     │   Merge     │     │   Check     │   │
│  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Consumer Trends Dataset - Cấu Trúc Dữ Liệu

### Các Series Chính Cần Extract

| Series ID | Mô Tả | Đơn Vị | Ý Nghĩa |
|-----------|-------|--------|---------|
| `ABJR` | Household final consumption expenditure: Total | £m CVM SA | Tổng chi tiêu hộ gia đình |
| `COICOP 05` | Furnishings, household equipment | £m CP NSA | Chi tiêu đồ nội thất/thiết bị |
| `UTIO` | HHFCE Implied Deflator | Index | Chỉ số giảm phát (IDEF) |
| `COICOP 09` | Recreation and culture | £m CP NSA | Chi tiêu giải trí |
| `COICOP 12` | Miscellaneous goods and services | £m CP NSA | Các hàng hóa/dịch vụ khác |

### Cột Dữ Liệu Cần Thiết

```
Year, Quarter, 
hfc_total_cvm_sa,       -- Tổng chi tiêu (CVM SA) - £m
hfc_05_cp_nsa,          -- Chi tiêu COICOP 05 - £m
hfc_idef_sa,            -- Chỉ số IDEF SA
hfc_09_cp_nsa,          -- Chi tiêu COICOP 09 - £m  
hfc_12_cp_nsa           -- Chi tiêu COICOP 12 - £m
```

---

## 🔄 DAG 1: Consumer Trends Data Ingestion

### Mục đích
Download và lưu trữ dữ liệu Consumer Trends từ ONS vào HDFS Raw Zone.

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                  DAG: consumer_trends_ingestion                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐         │
│  │   check_     │     │  download_   │     │  validate_   │         │
│  │   source_    │────▶│   ons_data   │────▶│   raw_data   │         │
│  │  available   │     │              │     │              │         │
│  └──────────────┘     └──────────────┘     └──────────────┘         │
│                              │                    │                  │
│                              ▼                    ▼                  │
│                       ┌──────────────┐     ┌──────────────┐         │
│                       │   upload_    │     │   log_       │         │
│                       │   to_hdfs    │────▶│   metadata   │         │
│                       │              │     │              │         │
│                       └──────────────┘     └──────────────┘         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Tasks Chi Tiết

#### Task 1: `check_source_available`
```python
def check_source_available(**context):
    """
    Kiểm tra ONS dataset có sẵn và có phiên bản mới không
    
    Actions:
    - HEAD request đến ONS download URL
    - So sánh Last-Modified header với lần download trước
    - Quyết định có cần download mới không
    """
    ONS_CSV_URL = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/satelliteaccounts/datasets/consumertrends/current/ct.csv"
    
    response = requests.head(ONS_CSV_URL, timeout=30)
    last_modified = response.headers.get('Last-Modified')
    
    # So sánh với metadata đã lưu
    # ...
    return {'needs_update': True, 'last_modified': last_modified}
```

#### Task 2: `download_ons_data`
```python
def download_ons_data(**context):
    """
    Download Consumer Trends CSV từ ONS
    
    Output:
    - ct.csv tạm thời lưu local
    - Metadata về file size, download time
    """
    ONS_CSV_URL = "https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/satelliteaccounts/datasets/consumertrends/current/ct.csv"
    
    response = requests.get(ONS_CSV_URL, stream=True, timeout=120)
    # Lưu tạm vào temp directory
    # ...
```

#### Task 3: `validate_raw_data`
```python
def validate_raw_data(**context):
    """
    Validate cấu trúc dữ liệu CSV
    
    Checks:
    - Số cột tối thiểu
    - Định dạng Year/Quarter
    - Các series ID cần thiết có mặt
    """
    df = pd.read_csv(local_file)
    
    required_patterns = [
        'Household final consumption expenditure',
        'Furnishings, household equipment',
        'IDEF'
    ]
    # Validate...
```

#### Task 4: `upload_to_hdfs`
```python
def upload_to_hdfs(**context):
    """
    Upload raw CSV vào HDFS Raw Zone
    
    Path: /data_lake/raw_zone/external/consumer_trends/
    Filename: ct_{download_date}.csv
    """
    hdfs_path = f"/data_lake/raw_zone/external/consumer_trends/ct_{date}.csv"
    upload_to_webhdfs(local_file, hdfs_path)
```

### DAG Configuration

```python
default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='consumer_trends_ingestion',
    default_args=default_args,
    description='Ingest Consumer Trends data from ONS to HDFS',
    schedule_interval='0 8 1 1,4,7,10 *',  # Quarterly: 1st day of Jan, Apr, Jul, Oct
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'ons', 'consumer-trends', 'macro'],
) as dag:
    # Tasks...
```

---

## 🔄 DAG 2: Consumer Trends Transformation

### Mục đích
Transform raw CSV thành format sạch, chuẩn hóa theo schema đã định nghĩa.

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                  DAG: consumer_trends_transform                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐                                                   │
│  │   read_      │                                                   │
│  │   raw_csv    │                                                   │
│  └──────┬───────┘                                                   │
│         │                                                            │
│         ▼                                                            │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐         │
│  │   parse_     │     │   select_    │     │  normalize_  │         │
│  │   time_cols  │────▶│   series     │────▶│   units      │         │
│  └──────────────┘     └──────────────┘     └──────────────┘         │
│                                                   │                  │
│                                                   ▼                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐         │
│  │   save_to_   │◀────│   create_    │◀────│   calc_      │         │
│  │   processed  │     │   dim_macro  │     │   derived    │         │
│  └──────────────┘     └──────────────┘     └──────────────┘         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Tasks Chi Tiết

#### Task 1: `parse_time_columns`
```python
def parse_time_columns(**context):
    """
    Parse cột thời gian từ format ONS (e.g., "2009 Q1")
    
    Output columns:
    - Year (int): 2009, 2010, ...
    - Quarter (int): 1, 2, 3, 4
    - Period (datetime): First day of quarter
    """
    df['Year'] = df['time_col'].apply(extract_year)
    df['Quarter'] = df['time_col'].apply(extract_quarter)
    df['Period'] = pd.to_datetime(df['Year'].astype(str) + 'Q' + df['Quarter'].astype(str))
```

#### Task 2: `select_series`
```python
def select_series(**context):
    """
    Select các series cần thiết theo keyword matching
    
    Series mapping:
    - 'Household final consumption expenditure :National concept CVM SA' → hfc_total_cvm_sa
    - '05 Furnishings, household equipment' → hfc_05_cp_nsa
    - 'IDEF SA' → hfc_idef_sa
    """
    series_mapping = {
        'hfc_total_cvm_sa': 'Household final consumption expenditure :National concept CVM SA',
        'hfc_05_cp_nsa': '05 Furnishings, household equipment',
        'hfc_idef_sa': 'Household final consumption expenditure: National concept IDEF SA',
        'hfc_09_cp_nsa': '09 Recreation and culture',
        'hfc_12_cp_nsa': '12 Miscellaneous goods and services'
    }
    # Fuzzy match columns...
```

#### Task 3: `calc_derived_features`
```python
def calc_derived_features(**context):
    """
    Tính các biến dẫn xuất từ dữ liệu gốc
    
    Derived features:
    - HFC_Growth_QoQ: Tăng trưởng chi tiêu theo quý
    - Inflation_Rate_QoQ: Tỷ lệ lạm phát theo quý
    - Log transformations
    """
    # Sort by time
    df = df.sort_values(['Year', 'Quarter'])
    
    # Calculate growth rates
    df['HFC_Growth_QoQ'] = df['hfc_total_cvm_sa'].pct_change()
    
    # Calculate inflation from IDEF
    df['hfc_idef_lag'] = df['hfc_idef_sa'].shift(1)
    df['Inflation_Rate_QoQ'] = (df['hfc_idef_sa'] - df['hfc_idef_lag']) / df['hfc_idef_lag']
    
    # Log transformations
    df['Log_hfc_total_cvm_sa'] = np.log(df['hfc_total_cvm_sa'])
```

#### Task 4: `create_dim_macro`
```python
def create_dim_macro(**context):
    """
    Tạo Dim_Macro dimension table
    
    Schema:
    - MacroKey (PK)
    - Year
    - Quarter  
    - Period
    - hfc_total_cvm_sa
    - hfc_05_cp_nsa
    - hfc_idef_sa
    - HFC_Growth_QoQ
    - Inflation_Rate_QoQ
    - Log_hfc_total_cvm_sa
    """
    dim_macro_cols = [
        'MacroKey', 'Year', 'Quarter', 'Period',
        'hfc_total_cvm_sa', 'hfc_05_cp_nsa', 'hfc_idef_sa',
        'hfc_09_cp_nsa', 'hfc_12_cp_nsa',
        'HFC_Growth_QoQ', 'Inflation_Rate_QoQ',
        'Log_hfc_total_cvm_sa'
    ]
```

### Output Schema: `Dim_Macro`

| Column | Type | Description |
|--------|------|-------------|
| `MacroKey` | INT (PK) | Surrogate key |
| `Year` | INT | Năm |
| `Quarter` | INT | Quý (1-4) |
| `Period` | TIMESTAMP | Ngày đầu quý |
| `hfc_total_cvm_sa` | FLOAT | Tổng chi tiêu HFC (CVM SA, £m) |
| `hfc_05_cp_nsa` | FLOAT | Chi tiêu COICOP 05 (£m) |
| `hfc_09_cp_nsa` | FLOAT | Chi tiêu COICOP 09 (£m) |
| `hfc_12_cp_nsa` | FLOAT | Chi tiêu COICOP 12 (£m) |
| `hfc_idef_sa` | FLOAT | Implicit Deflator Index |
| `HFC_Growth_QoQ` | FLOAT | Tăng trưởng HFC (%) |
| `Inflation_Rate_QoQ` | FLOAT | Tỷ lệ lạm phát (%) |
| `Log_hfc_total_cvm_sa` | FLOAT | Log của tổng chi tiêu |

---

## 🔄 DAG 3: Data Enrichment & Merge

### Mục đích
Merge dữ liệu Macro vào Star Schema hiện có, tạo fact table enriched.

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                  DAG: consumer_trends_enrichment                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐     ┌──────────────┐                              │
│  │   load_      │     │   load_      │                              │
│  │   dim_macro  │     │  fact_sales  │                              │
│  └──────┬───────┘     └──────┬───────┘                              │
│         │                    │                                       │
│         └────────┬───────────┘                                       │
│                  ▼                                                   │
│           ┌──────────────┐                                          │
│           │   create_    │                                          │
│           │ quarterly_   │                                          │
│           │   summary    │                                          │
│           └──────┬───────┘                                          │
│                  │                                                   │
│                  ▼                                                   │
│           ┌──────────────┐     ┌──────────────┐                     │
│           │   join_      │     │   create_    │                     │
│           │   macro_data │────▶│   lagged_    │                     │
│           │              │     │   features   │                     │
│           └──────────────┘     └──────┬───────┘                     │
│                                       │                              │
│                  ┌────────────────────┼────────────────────┐        │
│                  ▼                    ▼                    ▼        │
│           ┌──────────────┐     ┌──────────────┐     ┌──────────────┐│
│           │   save_      │     │   update_    │     │   create_    ││
│           │ fact_sales_  │     │ dim_customer │     │   fact_      ││
│           │  enriched    │     │              │     │  quarterly   ││
│           └──────────────┘     └──────────────┘     └──────────────┘│
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Tasks Chi Tiết

#### Task 1: `create_quarterly_summary`
```python
def create_quarterly_summary(**context):
    """
    Tổng hợp Fact_Sales theo Customer + Year + Quarter
    
    Aggregations:
    - Quarterly_Spend: SUM(TotalAmount)
    - Quarterly_Frequency: COUNT(DISTINCT Invoice)
    - Quarterly_Items: SUM(Quantity)
    - Avg_AOV_Quarterly: AVG(TotalAmount)
    """
    df_quarterly = df_sales.groupby(['CustomerID', 'Year', 'Quarter']).agg(
        Quarterly_Spend=('TotalAmount', 'sum'),
        Quarterly_Frequency=('Invoice', 'nunique'),
        Quarterly_Items=('Quantity', 'sum'),
        Avg_AOV_Quarterly=('TotalAmount', 'mean')
    ).reset_index()
```

#### Task 2: `join_macro_data`
```python
def join_macro_data(**context):
    """
    Join quarterly customer data với Dim_Macro
    
    Join key: Year + Quarter
    """
    df_enriched = pd.merge(
        df_quarterly,
        df_macro[['Year', 'Quarter', 'hfc_total_cvm_sa', 'hfc_idef_sa', 
                  'HFC_Growth_QoQ', 'Inflation_Rate_QoQ', 'Log_hfc_total_cvm_sa']],
        on=['Year', 'Quarter'],
        how='inner'
    )
```

#### Task 3: `create_lagged_features`
```python
def create_lagged_features(**context):
    """
    Tạo các biến lag cho mô hình
    
    Lagged features:
    - Lag_Spend: Chi tiêu quý trước
    - HFC_Growth_QoQ_Lagged: Tăng trưởng HFC quý trước
    - Is_FTB: First-Time Buyer indicator
    """
    df_enriched = df_enriched.sort_values(['CustomerID', 'Year', 'Quarter'])
    
    # Customer-level lags
    df_enriched['Lag_Spend'] = df_enriched.groupby('CustomerID')['Quarterly_Spend'].shift(1)
    df_enriched['Lag_Frequency'] = df_enriched.groupby('CustomerID')['Quarterly_Frequency'].shift(1)
    
    # Macro-level lags (same for all customers in a quarter)
    df_enriched['HFC_Growth_QoQ_Lagged'] = df_enriched.groupby('CustomerID')['HFC_Growth_QoQ'].shift(1)
    
    # First-Time Buyer flag
    EPSILON = 1e-6
    df_enriched['Is_FTB'] = np.where(df_enriched['Lag_Spend'].fillna(0) <= EPSILON, 1, 0)
    
    # Log transformations
    df_enriched['Log_Quarterly_Spend'] = np.log(df_enriched['Quarterly_Spend'].clip(lower=EPSILON))
    df_enriched['Log_Lag_Spend'] = np.log(df_enriched['Lag_Spend'].clip(lower=EPSILON))
```

### Output Schema: `Fact_Customer_Quarterly`

| Column | Type | Description |
|--------|------|-------------|
| `CustomerQuarterlyKey` | INT (PK) | Surrogate key |
| `CustomerKey` | INT (FK) | Ref to Dim_Customer |
| `MacroKey` | INT (FK) | Ref to Dim_Macro |
| `Year` | INT | Năm |
| `Quarter` | INT | Quý |
| `Quarterly_Spend` | FLOAT | Chi tiêu trong quý |
| `Quarterly_Frequency` | INT | Số lần mua trong quý |
| `Quarterly_Items` | INT | Số items mua trong quý |
| `Avg_AOV_Quarterly` | FLOAT | AOV trung bình quý |
| `Lag_Spend` | FLOAT | Chi tiêu quý trước |
| `Is_FTB` | INT | First-Time Buyer (0/1) |
| `Log_Quarterly_Spend` | FLOAT | Log chi tiêu |
| `Log_Lag_Spend` | FLOAT | Log chi tiêu quý trước |
| `HFC_Growth_QoQ_Lagged` | FLOAT | Tăng trưởng HFC quý trước |
| `Inflation_Rate_QoQ` | FLOAT | Lạm phát quý hiện tại |

---

## 🔄 DAG 4: Data Quality Check

### Mục đích
Validate data quality sau enrichment, đảm bảo tính toàn vẹn dữ liệu.

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                  DAG: consumer_trends_quality_check                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐         │
│  │   check_     │     │   check_     │     │   check_     │         │
│  │   nulls      │     │   outliers   │     │   referential│         │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘         │
│         │                    │                    │                  │
│         └────────────────────┼────────────────────┘                  │
│                              ▼                                       │
│                       ┌──────────────┐                              │
│                       │   generate_  │                              │
│                       │   quality_   │                              │
│                       │   report     │                              │
│                       └──────┬───────┘                              │
│                              │                                       │
│                              ▼                                       │
│                       ┌──────────────┐                              │
│                       │   alert_     │                              │
│                       │   if_failed  │                              │
│                       └──────────────┘                              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Quality Checks

#### 1. Null Check
```python
def check_nulls(**context):
    """
    Kiểm tra missing values trong các cột critical
    
    Critical columns:
    - CustomerID, Year, Quarter: 0% null allowed
    - Macro columns: 0% null allowed  
    - Lagged features: Max 10% null (expected for first quarters)
    """
    null_thresholds = {
        'CustomerID': 0.0,
        'Year': 0.0,
        'Quarter': 0.0,
        'hfc_total_cvm_sa': 0.0,
        'Lag_Spend': 0.15,  # Allow 15% for first quarters
        'HFC_Growth_QoQ_Lagged': 0.15
    }
```

#### 2. Outlier Check
```python
def check_outliers(**context):
    """
    Detect outliers trong các biến numeric
    
    Methods:
    - IQR method (1.5 * IQR)
    - Z-score > 3
    """
    numeric_cols = ['Quarterly_Spend', 'Quarterly_Frequency', 'hfc_total_cvm_sa']
    
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        outliers = df[(df[col] < Q1 - 1.5*IQR) | (df[col] > Q3 + 1.5*IQR)]
```

#### 3. Referential Integrity Check
```python
def check_referential(**context):
    """
    Kiểm tra foreign key constraints
    
    Checks:
    - Fact_Customer_Quarterly.CustomerKey → Dim_Customer.CustomerKey
    - Fact_Customer_Quarterly.MacroKey → Dim_Macro.MacroKey
    - Year/Quarter combinations exist in Dim_Macro
    """
    # Check orphan records...
```

---

## 📐 Star Schema Mới Sau Enrichment

```
                              ┌─────────────────────┐
                              │     Dim_Date        │
                              │  (Optional)         │
                              └─────────┬───────────┘
                                        │
┌─────────────────────┐                 │                 ┌─────────────────────┐
│    Dim_Customer     │                 │                 │     Dim_Product     │
│  (RFM + Segments)   │                 │                 │                     │
└─────────┬───────────┘                 │                 └─────────┬───────────┘
          │                             │                           │
          │         ┌───────────────────┴───────────────────┐       │
          │         │                                       │       │
          │         ▼                                       ▼       │
          │   ┌───────────────────┐               ┌───────────────────┐
          └──▶│   Fact_Sales      │               │Fact_Customer_     │◀──────┐
              │   (Transactions)  │               │  Quarterly        │       │
              └───────────────────┘               │ (Enriched)        │       │
                                                  └─────────┬─────────┘       │
                                                            │                 │
                                                            │                 │
                                                  ┌─────────▼─────────┐       │
                                                  │     Dim_Macro     │───────┘
                                                  │ (Consumer Trends) │
                                                  └───────────────────┘
```

---

## 📅 Scheduling Strategy

### DAG Dependencies

```
extract_historical_data (Manual/Once)
         │
         ▼
historical_data_etl_processing (Manual/Once)
         │
         ▼
consumer_trends_ingestion (Quarterly: Jan, Apr, Jul, Oct)
         │
         ▼
consumer_trends_transform (Triggered by ingestion)
         │
         ▼
consumer_trends_enrichment (Triggered by transform)
         │
         ▼
consumer_trends_quality_check (Triggered by enrichment)
```

### Cron Schedules

| DAG | Schedule | Trigger |
|-----|----------|---------|
| `consumer_trends_ingestion` | `0 8 1 1,4,7,10 *` | Quarterly |
| `consumer_trends_transform` | None | Dataset trigger |
| `consumer_trends_enrichment` | None | Dataset trigger |
| `consumer_trends_quality_check` | None | Dataset trigger |

---

## 🔧 Cấu Hình Environment Variables

```bash
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
POSTGRES_SCHEMA=public

# ONS Data Configuration
ONS_CT_CSV_URL=https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/satelliteaccounts/datasets/consumertrends/current/ct.csv
```

---

## 📁 HDFS Directory Structure

```
/data_lake/
├── raw_zone/
│   ├── internal_historical/        # Online Retail II data
│   │   └── year=YYYY/month=MM/
│   └── external/
│       └── consumer_trends/        # ONS Consumer Trends
│           ├── ct_2025-01-01.csv
│           └── ct_2025-04-01.csv
├── processed_zone/
│   ├── dim_macro/
│   │   └── dim_macro.parquet
│   └── fact_customer_quarterly/
│       └── enriched_data.parquet
└── curated_zone/
    └── analytics_ready/
        └── model_input.parquet
```

---

## 🚀 Implementation Roadmap

### Phase 1: Infrastructure Setup (Week 1)
- [ ] Tạo HDFS directories cho Consumer Trends
- [ ] Configure environment variables
- [ ] Test WebHDFS connectivity

### Phase 2: Ingestion DAG (Week 1-2)
- [ ] Implement `consumer_trends_ingestion` DAG
- [ ] Test download từ ONS
- [ ] Validate raw data storage

### Phase 3: Transform DAG (Week 2)
- [ ] Implement `consumer_trends_transform` DAG
- [ ] Create Dim_Macro table in PostgreSQL
- [ ] Test data quality

### Phase 4: Enrichment DAG (Week 2-3)
- [ ] Implement `consumer_trends_enrichment` DAG
- [ ] Create Fact_Customer_Quarterly table
- [ ] Validate join results

### Phase 5: Quality & Testing (Week 3)
- [ ] Implement quality check DAG
- [ ] End-to-end testing
- [ ] Documentation

---

## 📊 Metrics & Monitoring

### Key Metrics to Track

| Metric | Expected Value | Alert Threshold |
|--------|----------------|-----------------|
| ONS data freshness | ≤ 90 days | > 120 days |
| Join success rate | 100% | < 95% |
| Null rate in critical cols | 0% | > 0% |
| Processing time | < 30 min | > 60 min |
| Record count consistency | ±1% | > 5% variance |

### Alerting

```python
# Slack notification on failure
def alert_on_failure(context):
    """Send Slack alert when DAG fails"""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    
    message = f"""
    ⚠️ DAG Failed: {dag_id}
    Task: {task_id}
    Execution: {execution_date}
    """
    send_slack_alert(message)
```

---

## 📝 Notes & Considerations

### Data Considerations
1. **Time Alignment**: Online Retail II data (2009-2011) cần khớp với Consumer Trends cùng period
2. **Currency Units**: Consumer Trends dùng £m, cần chuẩn hóa với customer spend (£)
3. **Seasonality**: Q4 có thể có chi tiêu cao hơn do holiday season

### Technical Considerations
1. **Idempotency**: Các DAG phải idempotent, có thể re-run an toàn
2. **Backfill**: Hỗ trợ backfill cho historical data
3. **Error Handling**: Robust error handling với retry logic

### Model Considerations
1. **Lag Structure**: HFC_Growth cần lag 1 quý để tránh look-ahead bias
2. **Panel Data**: Data structure phù hợp cho PanelOLS Fixed Effects
3. **UK Filter**: Chỉ sử dụng customers từ United Kingdom
