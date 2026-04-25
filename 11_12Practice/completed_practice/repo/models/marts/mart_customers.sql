
with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_totals as (
    select order_id, sum(amount) as total_amount
    from payments
    group by
        1
),

customer_stats as (
    select
        orders.customer_id,
        count(orders.order_id) as total_orders,
        sum(order_totals.total_amount) as lifetime_value
    from orders
        left join order_totals on orders.order_id = order_totals.order_id
    group by
        1
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        coalesce(
            customer_stats.total_orders,
            0
        ) as total_orders,
        coalesce(
            customer_stats.lifetime_value,
            0
        ) as lifetime_value
    from customers
        left join customer_stats on customers.customer_id = customer_stats.customer_id
)

select * from final