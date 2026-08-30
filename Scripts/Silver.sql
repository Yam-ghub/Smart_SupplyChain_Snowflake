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

/*
- TRY_TO_...() everywhere instead of :: — bad values become NULL, not a crashed load
- Renamed Columns for clarity
- Dropped CUSTOMER_EMAIL, CUSTOMER_FNAME/LNAME, CUSTOMER_PASSWORD, CUSTOMER_STREET, PRODUCT_IMAGE, PRODUCT_DESCRIPTION — not needed for our analytics and it is only a generated synthetic PII data
- WHERE ORDER_ID IS NOT NULL — remove nulls since we can't identify orders that's null
- QUALIFY ROW_NUMBER() ... = 1 — this is the dedup, enforcing our grain (ORDER_ITEM_ID)
*/

-- We enable change tracking so this table can be used with a Stream. A Stream lets downstream steps read only the rows that are new or changed since the last time we checked, instead of reprocessing the entire table every run. This is standard practice for incremental pipelines — it saves compute and keeps runs fast, especially as the table grows.
ALTER TABLE SILVER_ORDERS SET CHANGE_TRACKING = TRUE;

-- Silver table changes validation
SELECT COUNT(*) FROM SILVER_ORDERS;

-- Should return 0 rows if our grain/dedup logic is correct
SELECT ORDER_ITEM_ID, COUNT(*) 
FROM SILVER_ORDERS
GROUP BY ORDER_ITEM_ID
HAVING COUNT(*) > 1;

-- Spot check a few rows
SELECT * FROM SILVER_ORDERS LIMIT 10;