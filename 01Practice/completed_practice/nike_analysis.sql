-- TO run :     duckdb :memory: ".read nike_analysis.sql"
WITH
    ranked_products AS (
        SELECT
            product_type,
            title,
            current_price,
            discount_percent,
            ROW_NUMBER() OVER (
                PARTITION BY
                    title
                ORDER BY
                    discount_percent DESC,
                    current_price ASC
            ) as rank
        FROM read_json_auto ('nike_discounts.json')
    )
SELECT
    product_type,
    title,
    current_price,
    discount_percent,
    rank
FROM ranked_products
WHERE
    rank <= 10
ORDER BY product_type, title, rank;