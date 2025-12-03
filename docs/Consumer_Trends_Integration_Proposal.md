# 📊 Đề Xuất Tích Hợp ONS Consumer Trends vào Data Warehouse

## 📋 Tổng Quan

### Về Dataset Consumer Trends
**Consumer Trends** là bộ dữ liệu chuỗi thời gian do **Office for National Statistics (ONS)** cung cấp, theo dõi **Household Final Consumption Expenditure (HHFCE)** - chi tiêu tiêu dùng cuối cùng của hộ gia đình tại UK.

#### Đặc điểm chính:
- **Tần suất:** Quarterly (phát hành ~90 ngày sau khi kết thúc quý)
- **Phạm vi:** UK National
- **Phân loại:** Theo **COICOP** (Classification of Individual Consumption by Purpose)
- **Định dạng:** CSV, XLSX, CSDB
- **URL Download:** https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/satelliteaccounts/datasets/consumertrends/current/ct.csv

#### Các phiên bản dữ liệu:
| Version | Mô tả | Khuyến nghị |
|---------|-------|-------------|
| **CVM SA** (Chained Volume Measure, Seasonally Adjusted) | Đã điều chỉnh lạm phát + mùa vụ | ✅ **Khuyến nghị** |
| CVM NSA | Đã điều chỉnh lạm phát, chưa điều chỉnh mùa vụ | ❌ |
| CP SA | Giá hiện hành, đã điều chỉnh mùa vụ | ❌ |
| CP NSA | Giá hiện hành, chưa điều chỉnh mùa vụ | ❌ |
| Implied Deflator | Chỉ số giảm phát | Bổ sung cho tính toán lạm phát |

---

## 🏗️ Kiến Trúc Hiện Tại

### Star Schema hiện tại:
```
                    ┌──────────────────┐
                    │   Fact_Sales     │
                    │ (Transactions)   │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Dim_Customer │    │ Dim_Product  │    │  Dim_Date    │
│ (RFM, CLV)   │    │              │    │  (implied)   │
└──────────────┘    └──────────────┘    └──────────────┘
```

### Tables hiện có:
- **Fact_Sales:** Transaction fact table
- **Dim_Customer:** CustomerID, RFM scores, Segments, CLV
- **Dim_Product:** ProductKey, StockCode, Description

---

## 🎯 Giải Pháp: Dimension Table Approach

### Tại sao chọn Dimension Table Approach?

| Tiêu chí | Đánh giá |
|----------|----------|
| **Chuẩn Star Schema** | ⭐⭐⭐⭐⭐ - Tuân thủ best practices, dễ maintain |
| **Flexibility** | ⭐⭐⭐⭐⭐ - Có thể join với bất kỳ fact table nào |
| **Storage** | ⭐⭐⭐⭐⭐ - Không duplicate data, hiệu quả |
| **Maintainability** | ⭐⭐⭐⭐⭐ - Dễ update khi ONS revise data |
| **Reusability** | ⭐⭐⭐⭐⭐ - Dùng được cho các analysis khác trong tương lai |

### Kiến trúc mở rộng

Thêm **Dim_Macro_Economic** như một **Slowly Changing Dimension (SCD Type 1)** kết nối qua **Dim_Date**.

```
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
│  (RFM, CLV)  │          │   (NEW!)     │              │              │
└──────────────┘          └──────┬───────┘              └──────────────┘
                                 │
                                 │ QuarterKey (FK)
                                 ▼
                    ┌────────────────────────┐
                    │  Dim_Macro_Economic    │
                    │   (Consumer Trends)    │
                    │        (NEW!)          │
                    └────────────────────────┘
```

### Relationships:
- **Fact_Sales** → **Dim_Date**: via `DateKey` (daily grain)
- **Fact_Sales** → **Dim_Macro_Economic**: via `QuarterKey` (quarterly grain)
- **Dim_Date** → **Dim_Macro_Economic**: via `QuarterKey` (for date-level queries)

### Lợi ích chính:
1. **Single Source of Truth** - Macro data chỉ lưu 1 nơi
2. **Easy Updates** - Khi ONS revise data, chỉ cần update Dim_Macro_Economic
3. **Future-proof** - Có thể mở rộng cho các fact tables khác
4. **Performance** - Data volume nhỏ (~100 rows), join không ảnh hưởng performance

---

## 📐 Schema Design Chi Tiết

### 1. Dim_Date (New - Conformed Dimension)

```sql
CREATE TABLE Dim_Date (
    DateKey         INT PRIMARY KEY,          -- YYYYMMDD format
    FullDate        DATE NOT NULL,
    Year            INT NOT NULL,
    Quarter         INT NOT NULL,             -- 1-4
    Month           INT NOT NULL,
    Day             INT NOT NULL,
    QuarterKey      VARCHAR(7) NOT NULL,      -- e.g., '2010-Q1'
    YearMonth       VARCHAR(7) NOT NULL,      -- e.g., '2010-01'
    DayOfWeek       INT,
    IsWeekend       BOOLEAN,
    FiscalYear      INT,
    FiscalQuarter   INT
);

-- Index for Quarter-level joins
CREATE INDEX idx_dim_date_quarter ON Dim_Date(QuarterKey);
```

### 2. Dim_Macro_Economic (New - Consumer Trends)

```sql
CREATE TABLE Dim_Macro_Economic (
    -- Primary Key
    QuarterKey          VARCHAR(7) PRIMARY KEY,   -- e.g., '2010-Q1'
    
    -- Time Attributes
    Year                INT NOT NULL,
    Quarter             INT NOT NULL,
    QuarterStartDate    DATE NOT NULL,
    QuarterEndDate      DATE NOT NULL,
    
    -- ═══════════════════════════════════════════════════════════════════
    -- HOUSEHOLD FINAL CONSUMPTION EXPENDITURE (HHFCE) - AGGREGATE
    -- ═══════════════════════════════════════════════════════════════════
    HFC_Total_CVM_SA            DECIMAL(18,2),    -- Total HHFCE (£m, CVM SA)
    HFC_Total_Growth_QoQ        DECIMAL(10,6),    -- Quarter-on-Quarter growth %
    HFC_Total_Growth_YoY        DECIMAL(10,6),    -- Year-on-Year growth %
    
    -- ═══════════════════════════════════════════════════════════════════
    -- COICOP CATEGORIES - Chi tiêu theo danh mục (£m, CVM SA)
    -- ═══════════════════════════════════════════════════════════════════
    -- COICOP 01: Food and non-alcoholic beverages
    COICOP_01_Food_NAB          DECIMAL(18,2),
    COICOP_01_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 02: Alcoholic beverages, tobacco and narcotics
    COICOP_02_Alcohol_Tobacco   DECIMAL(18,2),
    COICOP_02_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 03: Clothing and footwear
    COICOP_03_Clothing          DECIMAL(18,2),
    COICOP_03_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 04: Housing, water, electricity, gas
    COICOP_04_Housing           DECIMAL(18,2),
    COICOP_04_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 05: Furnishings, household equipment
    COICOP_05_Furnishings       DECIMAL(18,2),
    COICOP_05_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 06: Health
    COICOP_06_Health            DECIMAL(18,2),
    COICOP_06_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 07: Transport
    COICOP_07_Transport         DECIMAL(18,2),
    COICOP_07_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 08: Communication
    COICOP_08_Communication     DECIMAL(18,2),
    COICOP_08_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 09: Recreation and culture
    COICOP_09_Recreation        DECIMAL(18,2),
    COICOP_09_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 10: Education
    COICOP_10_Education         DECIMAL(18,2),
    COICOP_10_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 11: Restaurants and hotels
    COICOP_11_Restaurants       DECIMAL(18,2),
    COICOP_11_Growth_QoQ        DECIMAL(10,6),
    
    -- COICOP 12: Miscellaneous goods and services
    COICOP_12_Miscellaneous     DECIMAL(18,2),
    COICOP_12_Growth_QoQ        DECIMAL(10,6),
    
    -- ═══════════════════════════════════════════════════════════════════
    -- DERIVED METRICS
    -- ═══════════════════════════════════════════════════════════════════
    HFC_Lagged_1Q               DECIMAL(18,2),    -- HFC Total từ quý trước
    HFC_Growth_QoQ_Lagged       DECIMAL(10,6),    -- Growth từ quý trước (for models)
    
    -- ═══════════════════════════════════════════════════════════════════
    -- INFLATION & DEFLATOR (từ Implied Deflator dataset)
    -- ═══════════════════════════════════════════════════════════════════
    Implied_Deflator_Index      DECIMAL(10,4),    -- Index (2019=100)
    Inflation_Rate_QoQ          DECIMAL(10,6),    -- Quarter-on-Quarter inflation
    Inflation_Rate_YoY          DECIMAL(10,6),    -- Year-on-Year inflation
    
    -- ═══════════════════════════════════════════════════════════════════
    -- METADATA
    -- ═══════════════════════════════════════════════════════════════════
    Data_Source                 VARCHAR(50) DEFAULT 'ONS Consumer Trends',
    Release_Date                DATE,              -- Ngày ONS publish
    ETL_Loaded_At               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ETL_Updated_At              TIMESTAMP
);

-- Indexes
CREATE INDEX idx_macro_year_quarter ON Dim_Macro_Economic(Year, Quarter);
```

### 3. Cập nhật Fact_Sales

```sql
-- Add QuarterKey to Fact_Sales for joining
ALTER TABLE Fact_Sales 
ADD COLUMN QuarterKey VARCHAR(7);

-- Update existing data
UPDATE Fact_Sales 
SET QuarterKey = TO_CHAR(InvoiceDate, 'YYYY') || '-Q' || EXTRACT(QUARTER FROM InvoiceDate);

-- Create index for performance
CREATE INDEX idx_fact_sales_quarter ON Fact_Sales(QuarterKey);
```

### 4. Materialized View: Customer_Quarterly_Analysis (Optional - Performance Optimization)

Để tối ưu performance cho các use cases analytics thường xuyên, có thể tạo **Materialized View**:

```sql
CREATE MATERIALIZED VIEW mv_Customer_Quarterly_Analysis AS
SELECT 
    -- Customer Info
    c.CustomerID,
    c.RFM_Segment,
    c.R_Score, c.F_Score, c.M_Score,
    
    -- Time Period
    m.QuarterKey,
    m.Year,
    m.Quarter,
    
    -- Customer Quarterly Metrics (calculated)
    SUM(f.TotalAmount) AS Quarterly_Spend,
    COUNT(DISTINCT f.Invoice) AS Transaction_Count,
    AVG(f.TotalAmount) AS Avg_Transaction_Value,
    
    -- Macro Economic Context
    m.HFC_Total_CVM_SA,
    m.HFC_Total_Growth_QoQ,
    m.HFC_Growth_QoQ_Lagged,
    m.Inflation_Rate_QoQ,
    
    -- COICOP Categories (relevant for retail)
    m.COICOP_03_Clothing,           -- Online Retail sells clothing
    m.COICOP_05_Furnishings,        -- Home goods
    m.COICOP_09_Recreation,         -- Gifts, decorations
    m.COICOP_12_Miscellaneous,      -- General retail
    
    -- Derived for Analysis
    LOG(NULLIF(SUM(f.TotalAmount), 0)) AS Log_Quarterly_Spend,
    LOG(m.HFC_Total_CVM_SA) AS Log_HFC_Total

FROM Fact_Sales f
JOIN Dim_Customer c ON f.CustomerID = c.CustomerID
JOIN Dim_Macro_Economic m ON f.QuarterKey = m.QuarterKey
WHERE f.Country = 'United Kingdom'  -- Chỉ UK customers cho so sánh với UK macro
GROUP BY 
    c.CustomerID, c.RFM_Segment, c.R_Score, c.F_Score, c.M_Score,
    m.QuarterKey, m.Year, m.Quarter,
    m.HFC_Total_CVM_SA, m.HFC_Total_Growth_QoQ, m.HFC_Growth_QoQ_Lagged,
    m.Inflation_Rate_QoQ, m.COICOP_03_Clothing, m.COICOP_05_Furnishings,
    m.COICOP_09_Recreation, m.COICOP_12_Miscellaneous;

-- Refresh sau mỗi ETL run hoặc quarterly
REFRESH MATERIALIZED VIEW mv_Customer_Quarterly_Analysis;

-- Index cho performance
CREATE INDEX idx_mv_customer_quarter ON mv_Customer_Quarterly_Analysis(CustomerID, QuarterKey);
CREATE INDEX idx_mv_segment ON mv_Customer_Quarterly_Analysis(RFM_Segment);
```

**Lưu ý:** Materialized View này là **optional** - chỉ cần nếu query trực tiếp từ dimension tables quá chậm.
```

---

## 🔄 ETL Pipeline Design

### Data Flow:
```
┌────────────────────────────────────────────────────────────────────────────┐
│                    CONSUMER TRENDS ETL PIPELINE                             │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │   Crawl      │     │   Transform  │     │    Load      │                │
│  │ ONS Website  │────▶│  & Calculate │────▶│  PostgreSQL  │                │
│  │              │     │   Growth %   │     │              │                │
│  └──────────────┘     └──────────────┘     └──────────────┘                │
│        │                     │                    │                         │
│        ▼                     ▼                    ▼                         │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │ ct.csv from  │     │ Parse COICOP │     │ Dim_Macro_   │                │
│  │ ONS Dataset  │     │ Categories   │     │  Economic    │                │
│  └──────────────┘     └──────────────┘     └──────────────┘                │
│                                                                             │
│  Schedule: Quarterly (sau khi ONS release, ~90 ngày sau kết thúc quý)     │
│  Trigger: Manual hoặc cron @quarterly                                      │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

### Airflow DAG Structure:

```python
# Proposed DAG: extract_consumer_trends.py

"""
DAG: Extract ONS Consumer Trends Data
Schedule: Quarterly (manual trigger recommended)
Data: HHFCE by COICOP categories
"""

# Tasks:
# 1. download_consumer_trends_csv()
#    - Download ct.csv from ONS website
#    - Also download CVM SA specific file
#
# 2. parse_coicop_categories()
#    - Parse complex Excel/CSV structure
#    - Extract quarterly values for each COICOP category
#    - Handle time series ID mapping
#
# 3. calculate_derived_metrics()
#    - Calculate QoQ growth rates
#    - Calculate lagged values
#    - Calculate YoY growth
#
# 4. load_to_dim_macro_economic()
#    - Upsert to Dim_Macro_Economic table
#    - Update existing quarters if revisions
#
# 5. validate_data_quality()
#    - Check for missing quarters
#    - Validate growth rate calculations
#    - Compare with previous release
```

---

## 📊 COICOP Categories Mapping với Online Retail

### Relevant Categories cho Retail Analysis:

| COICOP | Category | Relevance | Use Case |
|--------|----------|-----------|----------|
| **01** | Food & Non-Alcoholic Beverages | Medium | Seasonal patterns |
| **02** | Alcohol & Tobacco | Low | Unless selling related |
| **03** | Clothing & Footwear | **HIGH** ⭐ | Online retail sells clothing |
| **04** | Housing, Water, Energy | Low | Background economic indicator |
| **05** | Furnishings & Household | **HIGH** ⭐ | Home goods, decorations |
| **06** | Health | Low | Unless health products |
| **07** | Transport | Low | Delivery cost proxy |
| **08** | Communication | Low | - |
| **09** | Recreation & Culture | **HIGH** ⭐ | Gifts, toys, decorations |
| **10** | Education | Low | Unless educational products |
| **11** | Restaurants & Hotels | Medium | Competitive spending |
| **12** | Miscellaneous | **HIGH** ⭐ | General retail proxy |

### Recommended Focus Categories:
1. **COICOP 03 (Clothing)** - Trực tiếp liên quan
2. **COICOP 05 (Furnishings)** - Home goods
3. **COICOP 09 (Recreation)** - Gifts, seasonal items
4. **COICOP 12 (Miscellaneous)** - General retail proxy
5. **Total HFC** - Overall consumer confidence

---

## 📈 Use Cases & Analysis Opportunities

### 1. Enriched Customer Spend Model (từ existing model)

```python
# Current model từ Customer_Spend_FE_Model_Overview.md:
Log_Quarterly_Spend = α_i + β₁·Log_Lag_Spend + β₂·Is_FTB 
                         + β₃·HFC_Growth_QoQ_Lagged + β₄·Inflation_Rate_QoQ + ε

# Enhanced model với COICOP categories:
Log_Quarterly_Spend = α_i + β₁·Log_Lag_Spend + β₂·Is_FTB 
                         + β₃·HFC_Growth_QoQ_Lagged 
                         + β₄·COICOP_09_Recreation_Growth  # NEW!
                         + β₅·COICOP_05_Furnishings_Growth # NEW!
                         + ε
```

### 2. Seasonality Analysis

```sql
-- So sánh seasonal patterns của khách hàng vs national trends
SELECT 
    m.Quarter,
    AVG(m.HFC_Total_Growth_QoQ) AS National_Avg_Growth,
    AVG(customer_quarterly_growth) AS Our_Customer_Growth,
    (AVG(customer_quarterly_growth) - AVG(m.HFC_Total_Growth_QoQ)) AS Outperformance
FROM mv_Customer_Quarterly_Analysis
GROUP BY m.Quarter
ORDER BY m.Quarter;
```

### 3. Segment Sensitivity Analysis

```sql
-- Phân tích segment nào nhạy cảm nhất với macro trends
SELECT 
    RFM_Segment,
    CORR(Quarterly_Spend, HFC_Total_Growth_QoQ) AS Correlation_With_HFC,
    CORR(Quarterly_Spend, Inflation_Rate_QoQ) AS Correlation_With_Inflation
FROM mv_Customer_Quarterly_Analysis
GROUP BY RFM_Segment
ORDER BY Correlation_With_HFC DESC;
```

### 4. Forecasting Input

```python
# Kết hợp với Phase 3 (CLV) và Phase 5 (Propensity)
# Macro factors có thể cải thiện prediction accuracy

features_for_propensity = [
    'R_Score', 'F_Score', 'M_Score',
    'Historical_CLV',
    'HFC_Growth_QoQ_Lagged',     # NEW: từ Consumer Trends
    'COICOP_09_Recreation_Growth' # NEW: category-specific
]
```

---

## 🚀 Implementation Roadmap

### Phase 1: Foundation (Week 1-2) ✅ **COMPLETED**
- [x] Tạo Dim_Date table - `extract_consumer_trends.py::task_create_dim_date()`
- [x] Tạo Dim_Macro_Economic table - `extract_consumer_trends.py::task_load_to_postgresql()`
- [x] Download và parse Consumer Trends CSV - `extract_consumer_trends.py::task_download_ons_data()`
- [x] Load historical data (2009-2011 cho Online Retail II period)

### Phase 2: ETL Pipeline (Week 2-3) ✅ **COMPLETED**
- [x] Develop Airflow DAG `extract_consumer_trends.py`
- [x] Implement COICOP parsing logic - `task_parse_coicop_categories()`
- [x] Add to HDFS Raw Zone: `/data_lake/raw_zone/external_ons_consumer_trends`
- [x] Update `historical_data_etl_processing.py` để thêm DateKey cho macro data join
- [x] Tạo `refresh_materialized_views.py` DAG cho maintenance

### Phase 3: Analytics Integration (Week 3-4) ✅ **COMPLETED**
- [x] Create Materialized Views - `mv_customer_quarterly_analysis`, `mv_segment_economic_performance`
- [x] Create analysis script - `experiments/Code/customer_spend_macro_analysis.py`
- [x] SQL schema documentation - `docs/schema/consumer_trends_schema.sql`
- [ ] Update existing models với macro features (pending)
- [ ] Validate model improvement (pending)

### Phase 4: Production (Week 4+) 🔄 **IN PROGRESS**
- [ ] Schedule quarterly refresh
- [ ] Set up data quality monitoring
- [ ] Create dashboard widgets

---

## 📁 Implemented Files

| File | Description |
|------|-------------|
| `pipelines/airflow/dags/extract_consumer_trends.py` | Main ETL DAG for ONS data |
| `pipelines/airflow/dags/refresh_materialized_views.py` | DAG to refresh MVs |
| `docs/schema/consumer_trends_schema.sql` | Database schema definitions |
| `experiments/Code/customer_spend_macro_analysis.py` | Analysis queries & panel data prep |

### How to Run:
```bash
# 1. Trigger Consumer Trends ETL
airflow dags trigger extract_consumer_trends

# 2. Run Historical ETL (includes DateKey)
airflow dags trigger historical_data_etl_processing

# 3. Refresh Materialized Views
airflow dags trigger refresh_materialized_views
```

---

## ⚠️ Considerations & Risks

### Data Quality:
- ONS data có thể bị **revision** → cần logic để handle updates
- Một số COICOP categories có thể có **forecast elements**

### Timing:
- Data release ~90 ngày sau quý kết thúc
- Cần consider **lag** khi dùng cho prediction

### Granularity Mismatch:
- Consumer Trends: Quarterly, National level
- Transaction data: Daily, Customer level
- → Join qua QuarterKey, chỉ áp dụng cho UK customers

### Currency:
- Consumer Trends: £ millions
- Transaction data: Individual £
- → Consider normalization hoặc dùng growth rates thay vì absolute values

---

## 📎 References

- [ONS Consumer Trends Dataset](https://www.ons.gov.uk/economy/nationalaccounts/satelliteaccounts/datasets/consumertrends)
- [Consumer Trends QMI (Methodology)](https://www.ons.gov.uk/economy/nationalaccounts/satelliteaccounts/methodologies/consumertrendsqmi)
- [COICOP Classification](http://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Classification_of_individual_consumption_by_purpose_(COICOP))
- [Download CVM SA Data](https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/satelliteaccounts/datasets/consumertrendschainedvolumemeasureseasonallyadjusted/current/cvmsaq225.xlsx)

---

*Document Created: December 2025*  
*Author: Data Engineering Team*  
*Version: 1.2 (Implemented)*  
*Approach: Dimension Table (Star Schema Extension)*
