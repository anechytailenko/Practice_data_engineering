-- file: 01_semi_structured_data.sql
-- Semi-structured data in DuckDB is usually stored as JSON or LIST/STRUCT types.
-- You typically: 1) store raw JSON, 2) extract fields with json_extract
-- 3) cast to proper SQL types, and 4) optionally model into normalized tables.

-- Enable JSON extension if needed (depends on DuckDB build)
-- INSTALL json;
-- LOAD json;

DROP TABLE IF EXISTS orders_raw;

CREATE TABLE orders_raw (
                            order_id      INTEGER,
                            raw_payload   JSON
);

INSERT INTO orders_raw VALUES
                           (1, '{"order_id":1,"customer":{"id":101,"name":"Alice"},"items":[{"sku":"A1","qty":2,"price":10.5},{"sku":"B2","qty":1,"price":5.0}],"total":26.0}'),
                           (2, '{"order_id":2,"customer":{"id":102,"name":"Bob"},"items":[{"sku":"A1","qty":1,"price":10.5}],"total":10.5}'),
                           (3, '{"order_id":3,"customer":{"id":101,"name":"Alice"},"items":[{"sku":"C3","qty":3,"price":7.0}],"total":21.0}');

-- Basic JSON inspection
SELECT
    order_id,
    raw_payload,
    json_type(raw_payload)              AS payload_type
FROM orders_raw;

-- Extract scalar fields from JSON into normal columns
SELECT
    order_id,
    json_extract_string(raw_payload, '$.customer.name')      AS customer_name,
    CAST(json_extract(raw_payload, '$.customer.id') AS INTEGER) AS customer_id,
    CAST(json_extract(raw_payload, '$.total')        AS DOUBLE)  AS total_amount
FROM orders_raw;

-- Extract nested array as JSON, then unnest to rows
-- Step 1: get items array
SELECT
    order_id,
    json_extract(raw_payload, '$.items') AS items_array
FROM orders_raw;
