# Smart_SupplyChain_Snowflake

## Setup
-- 1. Warehouse
```sql
CREATE WAREHOUSE IF NOT EXISTS CPG_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE CPG_WH;
```
-- 2. Database + medallion schemas
```sql
CREATE DATABASE IF NOT EXISTS CPG_SUPPLY_CHAIN;
USE DATABASE CPG_SUPPLY_CHAIN;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS UTILS;
```
-- 3. File format + stage
```sql
CREATE FILE FORMAT IF NOT EXISTS UTILS.CSV_FF
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  ENCODING = 'ISO-8859-1';

CREATE STAGE IF NOT EXISTS UTILS.SUPPLY_CHAIN_STAGE
  FILE_FORMAT = UTILS.CSV_FF;
```
## Bronze
### Creation of Bronze Table
```sql
DROP TABLE IF EXISTS BRONZE_ORDERS;
CREATE TABLE IF NOT EXISTS BRONZE_ORDERS (
    *COLUMN NAMES STRING DTYPE,
    _LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), --exact time this row was inserted
    _SOURCE_FILE STRING -- for data lineage if we ever load multiple files into this table
```

### Ingesting the Data to the Bronze_Orders table
```sql
COPY INTO BRONZE_ORDERS (
    *COLUMN NAMES
    _SOURCE_FILE-- destination column that receives METADATA$FILENAME below
FROM (
    -- $1 through $53 = source CSV columns, read by position (matches header order)
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
           $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,
           $39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,
           METADATA$FILENAME --- the 54th value: which file this row came from
   reading directly from a stage
    FROM @UTILS.SUPPLY_CHAIN_STAGE
    )
FILE_FORMAT = (FORMAT_NAME = UTILS.CSV_FF)
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';
)
```

## Data Validation and Exploration
### Ingestion Validation Checking

```sql
SELECT COUNT(*) FROM BRONZE_ORDERS;
SELECT * FROM BRONZE_ORDERS LIMIT 10;
DESCRIBE TABLE BRONZE_ORDERS;
```
### Finding the grain
- Checking the which is the grain
- ORDER_ID repeats across rows, but ORDER_ITEM_ID is unique per row. That tells me the table's grain is 'one order line item' and every single row in this table represents exactly one item within an order, not the whole order.

```sql
SELECT ORDER_ITEM_ID, COUNT(*) 
FROM BRONZE_ORDERS
GROUP BY ORDER_ITEM_ID
HAVING COUNT(*) > 1

SELECT ORDER_ID, COUNT(*) 
FROM BRONZE_ORDERS
GROUP BY ORDER_ID
HAVING COUNT(*) > 1
```

## Silver
### Objectives
- TRY_TO_...() everywhere instead of :: — bad values become NULL, not a crashed load
- Renamed Columns for clarity
- Dropped CUSTOMER_EMAIL, CUSTOMER_FNAME/LNAME, CUSTOMER_PASSWORD, CUSTOMER_STREET, PRODUCT_IMAGE, PRODUCT_DESCRIPTION — not needed for our analytics and it is only a generated synthetic PII data
- WHERE ORDER_ID IS NOT NULL — remove nulls since we can't identify orders that's null
- QUALIFY ROW_NUMBER() ... = 1 — this is the dedup, enforcing our grain (ORDER_ITEM_ID)

```sql
USE DATABASE CPG_SUPPLY_CHAIN;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE SILVER_ORDERS AS
SELECT
    -- Order & item identifiers (grain)
    TRY_TO_NUMBER(ORDER_ID) AS ORDER_ID,
    TRY_TO_NUMBER(ORDER_ITEM_ID) AS ORDER_ITEM_ID,

    -- Customer info 
    TRY_TO_NUMBER(CUSTOMER_ID) AS CUSTOMER_ID,
    CUSTOMER_SEGMENT AS CUSTOMER_SEGMENT,
    CUSTOMER_CITY AS CUSTOMER_CITY,
    CUSTOMER_STATE AS CUSTOMER_STATE,
    CUSTOMER_COUNTRY AS CUSTOMER_COUNTRY,

    -- Product info
    TRY_TO_NUMBER(PRODUCT_CARD_ID) AS PRODUCT_ID,
    PRODUCT_NAME AS PRODUCT_NAME,
    CATEGORY_NAME AS CATEGORY_NAME,
    DEPARTMENT_NAME AS DEPARTMENT_NAME,
    TRY_TO_DECIMAL(PRODUCT_PRICE, 12, 2) AS PRODUCT_PRICE,

    -- Order location/status
    MARKET AS MARKET,
    ORDER_REGION AS ORDER_REGION,
    ORDER_COUNTRY AS ORDER_COUNTRY,
    ORDER_STATE AS ORDER_STATE,
    ORDER_CITY AS ORDER_CITY,
    ORDER_STATUS AS ORDER_STATUS,
    
    -- Dates
    TRY_TO_TIMESTAMP_NTZ(ORDER_DATE_DATEORDERS, 'MM/DD/YYYY HH24:MI') AS ORDER_TS,
    TRY_TO_TIMESTAMP_NTZ(SHIPPING_DATE_DATEORDERS, 'MM/DD/YYYY HH24:MI') AS SHIPPING_TS,

    -- Shipping performance
    SHIPPING_MODE AS SHIPPING_MODE,
    TRY_TO_NUMBER(DAYS_FOR_SHIPPING_REAL) AS DAYS_SHIPPING_ACTUAL,
    TRY_TO_NUMBER(DAYS_FOR_SHIPMENT_SCHEDULED) AS DAYS_SHIPPING_SCHEDULED,
    IFF(TRY_TO_NUMBER(LATE_DELIVERY_RISK) = 1, TRUE, FALSE)AS IS_LATE_DELIVERY_RISK,
    DELIVERY_STATUS AS DELIVERY_STATUS,

    -- Financials (renamed for clarity)
    TRY_TO_NUMBER(ORDER_ITEM_QUANTITY) AS ORDER_ITEM_QUANTITY,
    TRY_TO_DECIMAL(SALES, 12, 2) AS SALES_AMOUNT,
    TRY_TO_DECIMAL(ORDER_ITEM_TOTAL, 12, 2) AS ORDER_ITEM_TOTAL,
    TRY_TO_DECIMAL(ORDER_ITEM_DISCOUNT, 12, 2) AS ORDER_ITEM_DISCOUNT,
    TRY_TO_DECIMAL(ORDER_ITEM_DISCOUNT_RATE, 6, 4) AS ORDER_ITEM_DISCOUNT_RATE,
    TRY_TO_DECIMAL(BENEFIT_PER_ORDER, 12, 2) AS PROFIT_PER_ORDER,
    TRY_TO_DECIMAL(ORDER_ITEM_PROFIT_RATIO, 6, 4) AS PROFIT_RATIO,

    -- Lineage (carried through from Bronze)
    _LOAD_TS,
    _SOURCE_FILE
    FROM BRONZE.BRONZE_ORDERS
    WHERE ORDER_ID IS NOT NULL          -- remove nulls since we can't identify orders that's null
    QUALIFY ROW_NUMBER() OVER(
    PARTITION BY ORDER_ITEM_ID      -- partition by grain
    ORDER BY _LOAD_TS DESC
    ) = 1;                          -- dedup'
```
```sql
  --this lets us later put a Stream on Silver too, so Gold can process only new/changed rows instead of full-refreshing.
    ALTER TABLE SILVER_ORDERS SET CHANGE_TRACKING = TRUE;
```
### Transformation Quality Check
```sql
    -- Silver table changes validation
    SELECT COUNT(*) FROM SILVER_ORDERS;

    -- Should return 0 rows if our grain/dedup logic is correct
    SELECT ORDER_ITEM_ID, COUNT(*) 
    FROM SILVER_ORDERS
    GROUP BY ORDER_ITEM_ID
    HAVING COUNT(*) > 1;

  -- Value spot check a few rows
    SELECT * FROM SILVER_ORDERS LIMIT 10;
```

## Gold Layer
```sql
  -- Creates DIM_DATE dimension table with date attributes for CPG supply chain analytics
  USE DATABASE CPG_SUPPLY_CHAIN;
  USE SCHEMA GOLD;

  CREATE OR REPLACE TABLE DIM_DATE AS
  SELECT 
      DATE_KEY,
      YEAR(DATE_KEY) AS YEAR,
      QUARTER(DATE_KEY) AS QUARTER,
      MONTH(DATE_KEY) AS MONTH,
      DAY(DATE_KEY) AS DAY_OF_MONTH,
      DAYOFWEEK(DATE_KEY) AS DAY_OF_WEEK,
      DAYNAME(DATE_KEY) AS DAY_NAME,
      IFF(DAYOFWEEK(DATE_KEY) IN (0, 6), TRUE, FALSE) AS IS_WEEKEND
  FROM(
      SELECT DATEADD(DAY, SEQ4(), '2015-01-01')::DATE AS DATE_KEY
      FROM TABLE(GENERATOR(ROWCOUNT => 3653))
      ) AS GENERATED_DATES -- ~10 years of datas from 2015-01-01
```
### DIM_DATE dimension validation
```sql
  
  SELECT COUNT(*) FROM DIM_DATE; 
  -- expect 3653

  SELECT MIN(DATE_KEY), MAX(DATE_KEY) FROM DIM_DATE;
  -- expect 2015-01-01 to roughly 2024-12-31

  SELECT * FROM DIM_DATE LIMIT 10; 
  -- column spotting
```
### Creates DIM_PRODUCT dimension table
```sql
CREATE OR REPLACE TABLE DIM_PRODUCT AS 
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY_NAME,
    DEPARTMENT_NAME,
    PRODUCT_PRICE
FROM (
    SELECT 
        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY_NAME,
        DEPARTMENT_NAME,
        PRODUCT_PRICE,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_ID
            ORDER BY _LOAD_TS DESC
        ) AS RN
    FROM SILVER.SILVER_ORDERS
) AS DEDUPED
WHERE RN = 1;
```
### Validation for DIM_PRODUCT
```sql
SELECT COUNT(*) FROM DIM_PRODUCT;
SELECT PRODUCT_ID, COUNT(*) FROM DIM_PRODUCT GROUP BY PRODUCT_ID HAVING COUNT(*) > 1;
SELECT * FROM DIM_PRODUCT LIMIT 10;
```
### Creates DIM_CUSTOMER dimension table
```sql
CREATE OR REPLACE TABLE DIM_CUSTOMER AS
SELECT
    CUSTOMER_ID,
    CUSTOMER_SEGMENT,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_COUNTRY
FROM (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_SEGMENT,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY _LOAD_TS DESC
        ) AS RN
    FROM SILVER.SILVER_ORDERS
) AS DEDUPED
WHERE RN = 1;

-- DIM_CUSTOMER validation
SELECT COUNT(*) FROM DIM_CUSTOMER;
SELECT * FROM DIM_CUSTOMER ORDER BY CUSTOMER_ID DESC LIMIT 20;
SELECT CUSTOMER_ID, COUNT(*) FROM DIM_CUSTOMER GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1;
```