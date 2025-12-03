# 📊 Hướng Dẫn Chạy Pipeline ONS Consumer Trends Integration

## 📋 Tổng Quan

Pipeline này tích hợp dữ liệu **ONS Consumer Trends** (macro-economic data) vào Data Warehouse, cho phép phân tích hành vi khách hàng trong bối cảnh kinh tế vĩ mô UK.

### Kiến Trúc

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA WAREHOUSE STAR SCHEMA                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                         ┌──────────────────┐                                │
│                         │   Fact_Sales     │                                │
│                         │  (Transactions)  │                                │
│                         └────────┬─────────┘                                │
│                                  │                                          │
│      ┌───────────────────────────┼───────────────────────────┐              │
│      │                           │                           │              │
│      ▼                           ▼                           ▼              │
│ ┌──────────────┐          ┌──────────────┐          ┌──────────────┐       │
│ │ Dim_Customer │          │   Dim_Date   │          │ Dim_Product  │       │
│ │  (RFM, CLV)  │          │  (NEW!)      │          │              │       │
│ └──────────────┘          └──────┬───────┘          └──────────────┘       │
│                                  │                                          │
│                                  │ QuarterKey                               │
│                                  ▼                                          │
│                    ┌─────────────────────────┐                              │
│                    │   Dim_Macro_Economic    │                              │
│                    │   (ONS Consumer Trends) │                              │
│                    │         (NEW!)          │                              │
│                    └─────────────────────────┘                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 Prerequisites

### 1. Services cần chạy
```bash
# Kiểm tra Docker services
sudo docker compose -f docker/docker-compose-anhth.yml ps

# Services cần có:
# - airflow-webserver (port 8081)
# - airflow-scheduler
# - postgres (port 5432)
# - namenode/datanode (HDFS - port 9870)
```

### 2. Data Warehouse đã có dữ liệu
- `Fact_Sales` - đã load từ `historical_data_etl_processing`
- `Dim_Customer` - đã có RFM segments
- `Dim_Product` - đã có product catalog

---

## 🚀 Các Bước Thực Hiện

### Step 1: Chạy Extract Consumer Trends (Tạo Dim_Date + Dim_Macro_Economic)

```bash
# Trigger DAG qua CLI
airflow dags trigger extract_consumer_trends

# Hoặc qua Airflow UI:
# 1. Mở http://localhost:8081
# 2. Tìm DAG "extract_consumer_trends"
# 3. Click "Trigger DAG"
```

**DAG này sẽ:**
- Download dữ liệu từ ONS website
- Parse COICOP categories (Food, Alcohol, Clothing, etc.)
- Tính toán growth rates (QoQ, YoY)
- Lưu vào HDFS: `/data_lake/raw_zone/external_ons_consumer_trends`
- Load vào PostgreSQL: `dim_macro_economic`, `dim_date`

**Output tables:**
| Table | Rows | Description |
|-------|------|-------------|
| `dim_date` | ~2,557 | Date dimension 2009-2015 |
| `dim_macro_economic` | ~12 | Quarterly macro data 2009-2011 |

---

### Step 2: Chạy Migration để thêm DateKey vào Fact_Sales

```bash
# Trigger migration DAG
airflow dags trigger migration_add_datekey
```

**Migration này sẽ:**
- `ALTER TABLE fact_sales ADD COLUMN datekey INT`
- `UPDATE fact_sales SET datekey = TO_CHAR(invoicedate, 'YYYYMMDD')::INT`
- `CREATE INDEX idx_fact_sales_datekey ON fact_sales(datekey)`

⚠️ **Lưu ý:** 
- Chạy 1 lần duy nhất
- Idempotent - chạy lại không bị lỗi
- KHÔNG cần re-run `historical_data_etl_processing`

---

### Step 3: (Optional) Tạo Materialized Views

```bash
# Trigger refresh views DAG
airflow dags trigger refresh_materialized_views
```

**Tạo các Materialized Views:**
- `mv_customer_quarterly_analysis` - Customer spending per quarter với macro context
- `mv_segment_economic_performance` - RFM segment performance by economic period

---

## ✅ Kiểm Tra Kết Quả

### Kết nối PostgreSQL và kiểm tra:

```bash
# Vào psql
psql -h 192.168.1.6 -U airflow -d retail_dw

# Hoặc dùng docker
sudo docker exec -it dmviz-postgres-1 psql -U airflow -d retail_dw
```

### Queries kiểm tra:

```sql
-- 1. Kiểm tra tables
SELECT table_name, 
       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as columns
FROM information_schema.tables t
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- 2. Kiểm tra Dim_Date
SELECT COUNT(*) as total_dates, 
       MIN(fulldate) as min_date, 
       MAX(fulldate) as max_date,
       COUNT(DISTINCT quarterkey) as quarters
FROM dim_date;

-- 3. Kiểm tra Dim_Macro_Economic
SELECT quarterkey, year, quarter, 
       hfc_total_cvm_sa, 
       hfc_total_growth_qoq
FROM dim_macro_economic
ORDER BY year, quarter;

-- 4. Kiểm tra DateKey trong Fact_Sales
SELECT COUNT(*) as total,
       COUNT(datekey) as with_datekey,
       COUNT(*) - COUNT(datekey) as without_datekey
FROM fact_sales;

-- 5. Test JOIN giữa các tables
SELECT 
    dd.quarterkey,
    COUNT(DISTINCT fs.customerkey) as customers,
    SUM(fs.totalamount) as total_sales,
    me.hfc_total_cvm_sa as national_consumption
FROM fact_sales fs
JOIN dim_date dd ON fs.datekey = dd.datekey
LEFT JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
GROUP BY dd.quarterkey, me.hfc_total_cvm_sa
ORDER BY dd.quarterkey;
```

---

## 📅 Schedule Tự Động

DAG `extract_consumer_trends` được cấu hình chạy tự động theo quý:

| Cron Expression | `0 6 15 1,4,7,10 *` |
|-----------------|---------------------|
| **Thời gian** | 6:00 AM ngày 15 |
| **Tháng** | Tháng 1, 4, 7, 10 |
| **Giải thích** | Đầu mỗi quý, sau khi ONS release data |

### Enable schedule:
```bash
# Enable DAG schedule
airflow dags unpause extract_consumer_trends
```

---

## 🔍 Troubleshooting

### Lỗi: "ModuleNotFoundError: No module named 'migrations'"

```bash
# Kiểm tra file __init__.py
ls -la pipelines/airflow/dags/migrations/

# Phải có:
# - __init__.py
# - add_datekey_to_fact_sales.py
```

### Lỗi: "Connection refused to PostgreSQL"

```bash
# Kiểm tra PostgreSQL service
sudo docker compose -f docker/docker-compose-anhth.yml ps postgres

# Kiểm tra port
nc -zv 192.168.1.6 5432
```

### Lỗi: "Table dim_date/dim_macro_economic not found"

```bash
# Chắc chắn đã chạy Step 1 trước
airflow dags trigger extract_consumer_trends

# Kiểm tra task status
airflow tasks list extract_consumer_trends --tree
```

### Lỗi: "DateKey column already exists"

```bash
# Không phải lỗi - migration đã chạy rồi
# Script là idempotent, chạy lại không sao
```

---

## 📁 File Structure

```
pipelines/airflow/dags/
├── extract_consumer_trends.py      # Main DAG - load ONS data
├── migration_add_datekey.py        # One-time migration DAG
├── refresh_materialized_views.py   # Refresh MVs DAG
└── migrations/
    ├── __init__.py
    └── add_datekey_to_fact_sales.py  # Migration logic

docs/
├── Consumer_Trends_Integration_Proposal.md  # Full proposal document
└── schema/
    └── consumer_trends_schema.sql           # SQL schema definitions

experiments/Code/
└── customer_spend_macro_analysis.py         # Analysis queries
```

---

## 📊 Sample Analysis Queries

### Query 1: Customer Spending vs National Trends
```sql
SELECT 
    dc.rfm_segment,
    dd.quarterkey,
    COUNT(DISTINCT dc.customerid) AS customer_count,
    SUM(fs.totalamount) AS segment_spend,
    me.hfc_total_growth_qoq AS national_growth
FROM fact_sales fs
JOIN dim_customer dc ON fs.customerkey = dc.customerkey
JOIN dim_date dd ON fs.datekey = dd.datekey
LEFT JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
GROUP BY dc.rfm_segment, dd.quarterkey, me.hfc_total_growth_qoq
ORDER BY dd.quarterkey, segment_spend DESC;
```

### Query 2: Segment Sensitivity to Economic Changes
```sql
SELECT 
    dc.rfm_segment,
    CASE 
        WHEN me.hfc_total_growth_qoq >= 0 THEN 'Growth Period'
        ELSE 'Contraction Period'
    END AS economic_period,
    COUNT(DISTINCT dc.customerid) AS customers,
    SUM(fs.totalamount) AS total_revenue
FROM fact_sales fs
JOIN dim_customer dc ON fs.customerkey = dc.customerkey
JOIN dim_date dd ON fs.datekey = dd.datekey
LEFT JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
WHERE me.hfc_total_growth_qoq IS NOT NULL
GROUP BY dc.rfm_segment, 
    CASE WHEN me.hfc_total_growth_qoq >= 0 THEN 'Growth Period' ELSE 'Contraction Period' END
ORDER BY total_revenue DESC;
```

---

## 📝 Quick Reference

| Action | Command |
|--------|---------|
| Trigger Consumer Trends ETL | `airflow dags trigger extract_consumer_trends` |
| Run DateKey Migration | `airflow dags trigger migration_add_datekey` |
| Refresh Materialized Views | `airflow dags trigger refresh_materialized_views` |
| Enable Quarterly Schedule | `airflow dags unpause extract_consumer_trends` |
| Check DAG Status | `airflow dags list` |
| View DAG Runs | `airflow dags list-runs -d extract_consumer_trends` |

---

*Created: December 2025*
*Author: Data Engineering Team*
