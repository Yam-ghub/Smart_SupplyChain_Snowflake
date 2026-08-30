# Smart_SupplyChain_Snowflake

## Bronze
- Creation of Bronze Table
```sql
DROP TABLE IF EXISTS BRONZE_ORDERS;
CREATE TABLE IF NOT EXISTS BRONZE_ORDERS (
    *COLUMN NAMES STRING DTYPE,
    _LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE STRING
```
- Loading/Ingesting the Data to the Bronze_Orders table
```sql
COPY INTO BRONZE_ORDERS (
    *COLUMN NAMES
FROM (
    --$Column number in CSV
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
           $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,
           $39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,
           METADATA$FILENAME
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

