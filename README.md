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

