{{
    config(
        materialized='incremental',
        unique_key='order_date'
    )
}}

with orders as ( select * from {{ ref('stg_orders') }} )


select
    order_date,
    count(order_id) as daily_order_count,
    count(distinct customer_id) as unique_customers
from orders

{% if is_incremental() %}
    where order_date >= (select max(order_date) from {{ this }})
{% endif %}

group by 1