# Smart_SupplyChain_Snowflake

## Bronze
- Creation of Bronze Table
- Loading/Ingesting the Data to the Bronze_Orders table

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

