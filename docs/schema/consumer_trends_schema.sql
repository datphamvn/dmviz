-- ============================================================================
-- Consumer Trends Integration - Database Schema
-- ============================================================================
-- Version: 1.0
-- Author: Data Engineering Team
-- Created: December 2025
-- Description: Schema definitions for ONS Consumer Trends integration
--              following the Dimension Table Approach
-- ============================================================================

-- ============================================================================
-- 1. DIM_DATE - Conformed Date Dimension with QuarterKey
-- ============================================================================
-- Purpose: Links daily transactions to quarterly macro-economic data

CREATE TABLE IF NOT EXISTS dim_date (
    DateKey INT PRIMARY KEY,                    -- YYYYMMDD format
    FullDate DATE NOT NULL,                     -- Actual date
    Year INT NOT NULL,
    Quarter INT NOT NULL,                       -- 1, 2, 3, 4
    Month INT NOT NULL,
    Day INT NOT NULL,
    QuarterKey VARCHAR(7) NOT NULL,             -- Format: YYYY-Q# (e.g., 2010-Q1)
    YearMonth VARCHAR(7) NOT NULL,              -- Format: YYYY-MM
    DayOfWeek INT,                              -- 0=Monday, 6=Sunday
    IsWeekend BOOLEAN,
    FiscalYear INT,
    FiscalQuarter INT,
    
    -- Indexes for common queries
    CONSTRAINT chk_quarter CHECK (Quarter >= 1 AND Quarter <= 4),
    CONSTRAINT chk_month CHECK (Month >= 1 AND Month <= 12)
);

CREATE INDEX IF NOT EXISTS idx_dim_date_quarter ON dim_date(QuarterKey);
CREATE INDEX IF NOT EXISTS idx_dim_date_fulldate ON dim_date(FullDate);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(Year, Month);


-- ============================================================================
-- 2. DIM_MACRO_ECONOMIC - Quarterly Macro Economic Indicators
-- ============================================================================
-- Purpose: Stores ONS Consumer Trends data (HHFCE by COICOP categories)

CREATE TABLE IF NOT EXISTS dim_macro_economic (
    -- Primary Key
    QuarterKey VARCHAR(7) PRIMARY KEY,          -- Format: YYYY-Q# (e.g., 2010-Q1)
    
    -- Time Attributes
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    QuarterStartDate DATE,
    QuarterEndDate DATE,
    
    -- Total Household Final Consumption Expenditure (£ millions, CVM SA)
    HFC_Total_CVM_SA DECIMAL(18,2),             -- Total HHFCE
    HFC_Total_Growth_QoQ DECIMAL(10,6),         -- Quarter-on-Quarter growth rate
    HFC_Total_Growth_YoY DECIMAL(10,6),         -- Year-on-Year growth rate
    
    -- COICOP Categories (£ millions, CVM SA)
    COICOP_01_Food_NAB DECIMAL(18,2),           -- Food and non-alcoholic beverages
    COICOP_02_Alcohol_Tobacco DECIMAL(18,2),    -- Alcoholic beverages, tobacco, narcotics
    COICOP_03_Clothing DECIMAL(18,2),           -- Clothing and footwear
    COICOP_04_Housing DECIMAL(18,2),            -- Housing, water, electricity, gas
    COICOP_05_Furnishings DECIMAL(18,2),        -- Furnishings, household equipment
    COICOP_06_Health DECIMAL(18,2),             -- Health
    COICOP_07_Transport DECIMAL(18,2),          -- Transport
    COICOP_08_Communication DECIMAL(18,2),      -- Communication
    COICOP_09_Recreation DECIMAL(18,2),         -- Recreation and culture
    COICOP_10_Education DECIMAL(18,2),          -- Education
    COICOP_11_Restaurants DECIMAL(18,2),        -- Restaurants and hotels
    COICOP_12_Miscellaneous DECIMAL(18,2),      -- Miscellaneous goods and services
    
    -- Lagged Values (for regression models)
    HFC_Lagged_1Q DECIMAL(18,2),                -- Previous quarter value
    HFC_Growth_QoQ_Lagged DECIMAL(10,6),        -- Previous quarter growth
    
    -- Inflation/Deflator
    Implied_Deflator_Index DECIMAL(10,4),
    Inflation_Rate_QoQ DECIMAL(10,6),
    Inflation_Rate_YoY DECIMAL(10,6),
    
    -- Metadata
    Data_Source VARCHAR(50) DEFAULT 'ONS Consumer Trends',
    ETL_Loaded_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ETL_Updated_At TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_macro_quarter CHECK (Quarter >= 1 AND Quarter <= 4)
);

CREATE INDEX IF NOT EXISTS idx_dim_macro_year ON dim_macro_economic(Year);
CREATE INDEX IF NOT EXISTS idx_dim_macro_year_quarter ON dim_macro_economic(Year, Quarter);


-- ============================================================================
-- 3. UPDATE FACT_SALES - Add DateKey Foreign Key
-- ============================================================================
-- Note: Run this only once to add the DateKey column

-- Step 1: Add DateKey column if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'fact_sales' AND column_name = 'datekey'
    ) THEN
        ALTER TABLE fact_sales ADD COLUMN DateKey INT;
    END IF;
END $$;

-- Step 2: Populate DateKey from InvoiceDate
UPDATE fact_sales 
SET DateKey = TO_CHAR(invoicedate, 'YYYYMMDD')::INT
WHERE DateKey IS NULL;

-- Step 3: Create index on DateKey
CREATE INDEX IF NOT EXISTS idx_fact_sales_datekey ON fact_sales(datekey);


-- ============================================================================
-- 4. MATERIALIZED VIEW - Customer Quarterly Analysis (Optional - Performance)
-- ============================================================================
-- Purpose: Pre-aggregated view joining customer transactions with macro data
-- Note: This is optional and can be created when needed for performance

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_quarterly_analysis AS
SELECT 
    -- Customer Identifiers
    dc.customerkey,
    dc.customerid,
    dc.rfm_segment,
    
    -- Time Dimension
    dd.year,
    dd.quarter,
    dd.quarterkey,
    
    -- Customer Quarterly Metrics
    COUNT(DISTINCT fs.invoice) AS quarterly_orders,
    SUM(fs.quantity) AS quarterly_items,
    SUM(fs.totalamount) AS quarterly_spend,
    AVG(fs.totalamount) AS avg_order_value,
    
    -- Macro Economic Context
    me.hfc_total_cvm_sa AS national_hfc_total,
    me.hfc_total_growth_qoq AS national_hfc_growth,
    me.coicop_02_alcohol_tobacco AS national_alcohol_spend,
    me.coicop_11_restaurants AS national_restaurants_spend,
    
    -- Calculated Metrics
    CASE 
        WHEN me.hfc_total_cvm_sa > 0 
        THEN SUM(fs.totalamount) / me.hfc_total_cvm_sa * 100
        ELSE 0 
    END AS spend_vs_national_pct
    
FROM fact_sales fs
JOIN dim_customer dc ON fs.customerkey = dc.customerkey
JOIN dim_date dd ON fs.datekey = dd.datekey
LEFT JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
GROUP BY 
    dc.customerkey,
    dc.customerid,
    dc.rfm_segment,
    dd.year,
    dd.quarter,
    dd.quarterkey,
    me.hfc_total_cvm_sa,
    me.hfc_total_growth_qoq,
    me.coicop_02_alcohol_tobacco,
    me.coicop_11_restaurants;

-- Create indexes on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cqa_customer_quarter 
ON mv_customer_quarterly_analysis(customerkey, quarterkey);

CREATE INDEX IF NOT EXISTS idx_mv_cqa_segment 
ON mv_customer_quarterly_analysis(rfm_segment);

-- Refresh command (run after ETL completes)
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_quarterly_analysis;


-- ============================================================================
-- 5. SAMPLE ANALYSIS QUERIES
-- ============================================================================

-- Query 1: Customer Spending vs National Trends
-- Usage: Identify customers whose spending correlates with or diverges from
-- national consumption trends

/*
SELECT 
    dc.rfm_segment,
    dd.quarterkey,
    COUNT(DISTINCT dc.customerid) AS customer_count,
    SUM(fs.totalamount) AS segment_spend,
    AVG(me.hfc_total_growth_qoq) AS avg_national_growth,
    AVG(fs.totalamount) AS avg_customer_spend
FROM fact_sales fs
JOIN dim_customer dc ON fs.customerkey = dc.customerkey
JOIN dim_date dd ON fs.datekey = dd.datekey
LEFT JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
GROUP BY dc.rfm_segment, dd.quarterkey
ORDER BY dd.quarterkey, segment_spend DESC;
*/

-- Query 2: Seasonal Analysis with Macro Context
-- Usage: Compare customer behavior against national consumption patterns

/*
SELECT 
    dd.year,
    dd.quarter,
    me.hfc_total_cvm_sa AS national_consumption,
    me.hfc_total_growth_qoq AS national_growth,
    SUM(fs.totalamount) AS total_sales,
    COUNT(DISTINCT fs.customerkey) AS active_customers,
    SUM(fs.totalamount) / NULLIF(me.hfc_total_cvm_sa, 0) * 1000000 AS sales_per_million_national
FROM fact_sales fs
JOIN dim_date dd ON fs.datekey = dd.datekey
LEFT JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
GROUP BY dd.year, dd.quarter, me.hfc_total_cvm_sa, me.hfc_total_growth_qoq
ORDER BY dd.year, dd.quarter;
*/

-- Query 3: RFM Segment Performance with Economic Context
-- Usage: Analyze segment performance during different economic periods

/*
SELECT 
    dc.rfm_segment,
    CASE 
        WHEN me.hfc_total_growth_qoq >= 0 THEN 'Growth Period'
        ELSE 'Contraction Period'
    END AS economic_period,
    COUNT(DISTINCT dc.customerid) AS customers,
    AVG(dc.monetary) AS avg_clv,
    SUM(fs.totalamount) AS total_revenue
FROM fact_sales fs
JOIN dim_customer dc ON fs.customerkey = dc.customerkey
JOIN dim_date dd ON fs.datekey = dd.datekey
LEFT JOIN dim_macro_economic me ON dd.quarterkey = me.quarterkey
GROUP BY dc.rfm_segment, 
    CASE 
        WHEN me.hfc_total_growth_qoq >= 0 THEN 'Growth Period'
        ELSE 'Contraction Period'
    END
ORDER BY total_revenue DESC;
*/


-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE dim_date IS 'Conformed Date Dimension for Star Schema - links daily transactions to quarterly macro data';
COMMENT ON TABLE dim_macro_economic IS 'ONS Consumer Trends macro-economic indicators by quarter (COICOP categories)';
COMMENT ON MATERIALIZED VIEW mv_customer_quarterly_analysis IS 'Pre-aggregated customer quarterly analysis with macro context (optional performance optimization)';

COMMENT ON COLUMN dim_macro_economic.QuarterKey IS 'Primary key in YYYY-Q# format for joining with Dim_Date';
COMMENT ON COLUMN dim_macro_economic.HFC_Total_CVM_SA IS 'Total Household Final Consumption Expenditure in £ millions (Chained Volume Measure, Seasonally Adjusted)';
COMMENT ON COLUMN dim_macro_economic.COICOP_02_Alcohol_Tobacco IS 'COICOP Category 02: Alcoholic beverages, tobacco and narcotics';
