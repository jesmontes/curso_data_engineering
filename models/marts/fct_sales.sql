--FCT SALES QUITANDO CAMPOS INNECESARIOS

WITH int_orders_order_items_grouped AS(
SELECT *
    FROM {{ ref('int_orders_order_items_grouped') }}
), 
sales AS (
SELECT  
        user_id,
        order_id,
        address_id,
        promo_id,
        created_at_utc,
        delivered_at_utc,
        product_id,
        quantity,
        price_product_eur,
        order_cost_by_item_eur,
        shipping_cost_by_item_eur,        
        order_cost_and_shipping_by_item_eur,
        discount_by_item_eur,
        order_total_by_item,
        DATEDIFF(DAY,created_at_utc,delivered_at_utc) AS shipping_duration_days

       FROM int_orders_order_items_grouped
)
SELECT * FROM sales
