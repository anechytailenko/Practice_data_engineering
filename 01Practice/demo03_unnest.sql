-- file: 03_unnest.sql
-- UNNEST in DuckDB turns array (LIST) values into a set of rows.
-- Typical flow: 1) store array-typed column (e.g., VARCHAR[]),
-- 2) UNNEST to explode it into rows,
-- 3) optionally aggregate / join results.

DROP TABLE IF EXISTS user_events;

CREATE TABLE user_events (
                             user_id      INTEGER,
                             session_id   INTEGER,
                             event_types  VARCHAR[]
);

INSERT INTO user_events VALUES
                            (1, 1001, ['page_view', 'click', 'scroll']),
                            (1, 1002, ['page_view', 'page_view']),
                            (2, 2001, ['page_view', 'click']),
                            (3, 3001, []::VARCHAR[]),     -- explicit cast for empty array
                            (4, 4001, ['signup']);

-- 1) Basic UNNEST: one row per event type
-- Syntax: FROM table, UNNEST(array_column) AS alias(column_name)
SELECT
    user_id,
    session_id,
    event_type
FROM user_events,
     UNNEST(event_types) AS t(event_type)
ORDER BY user_id, session_id, event_type;

-- 2) Count events per user after unnest
WITH exploded AS (
    SELECT
        user_id,
        event_type
    FROM user_events,
         UNNEST(event_types) AS t(event_type)
)
SELECT
    user_id,
    COUNT(*) AS total_events,
    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS click_events
FROM exploded
GROUP BY user_id
ORDER BY user_id;

-- 4) Another example: explode product_ids per order
DROP TABLE IF EXISTS orders_items_list;

CREATE TABLE orders_items_list (
                                   order_id    INTEGER,
                                   customer_id INTEGER,
                                   product_ids INTEGER[]
);

INSERT INTO orders_items_list VALUES
                                  (10, 101, [1, 2, 3]),
                                  (11, 102, [2, 4]),
                                  (12, 101, [3]);

SELECT
    order_id,
    customer_id,
    product_id
FROM orders_items_list,
     UNNEST(product_ids) AS t(product_id)
ORDER BY order_id, product_id;
