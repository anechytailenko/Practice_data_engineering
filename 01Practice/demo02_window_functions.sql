-- file: 02_window_functions.sql
-- Window functions process sets of rows related to the current row (partition),
-- without collapsing them like GROUP BY. Common patterns: ranking, running totals,
-- moving averages, lag/lead for deltas, and percentiles.

DROP TABLE IF EXISTS daily_sales;

CREATE TABLE daily_sales (
                             sale_date   DATE,
                             region      VARCHAR,
                             customer_id INTEGER,
                             amount      DOUBLE
);

INSERT INTO daily_sales VALUES
                            ('2025-01-01', 'North', 1, 100.0),
                            ('2025-01-01', 'North', 2, 150.0),
                            ('2025-01-02', 'North', 1, 200.0),
                            ('2025-01-02', 'North', 2,  50.0),
                            ('2025-01-03', 'North', 1, 120.0),
                            ('2025-01-01', 'South', 3,  80.0),
                            ('2025-01-02', 'South', 3,  90.0),
                            ('2025-01-03', 'South', 4, 300.0);

-- 1) Row numbering per region, ordered by date then amount
SELECT
    region,
    sale_date,
    amount,
    ROW_NUMBER() OVER (
        PARTITION BY region
        ORDER BY sale_date, amount DESC
    ) AS rn_in_region
FROM daily_sales
ORDER BY region, sale_date, amount DESC;

-- 2) Running total (cumulative sum) per customer over time
SELECT
    customer_id,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_customer
FROM daily_sales
ORDER BY customer_id, sale_date;

-- 3) Daily total per region vs each row (window SUM vs GROUP BY)
SELECT
    region,
    sale_date,
    customer_id,
    amount,
    SUM(amount) OVER (PARTITION BY region, sale_date) AS daily_region_total
FROM daily_sales
ORDER BY region, sale_date, customer_id;

-- 4) lag/lead: difference vs previous day per customer
SELECT
    customer_id,
    sale_date,
    amount,
    LAG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY sale_date
    ) AS prev_amount,
    amount - COALESCE(
            LAG(amount) OVER (
                    PARTITION BY customer_id
            ORDER BY sale_date
                        ), 0
             ) AS delta_vs_prev
FROM daily_sales
ORDER BY customer_id, sale_date;

-- 5) Moving 3-row average per region sorted by date
SELECT
    region,
    sale_date,
    amount,
    AVG(amount) OVER (
        PARTITION BY region
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_rows
FROM daily_sales
ORDER BY region, sale_date;
