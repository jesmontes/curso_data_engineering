{{
  config(
    materialized='table'
  )
}}

WITH int_sales AS (
    SELECT * 
    FROM {{ ref('int_orders_order_items_grouped') }}
    )
SELECT user_id,
        order_id,
        address_id,
        DATE(created_at_utc) AS created_at_utc,
        DATE(delivered_at_utc) AS delivered_at_utc,
        product_id,
        quantity,
        price_product_eur,
        order_cost_by_item_eur,
        shipping_cost_by_item_eur,        
        order_cost_and_shipping_by_item_eur,
        discount_by_item_eur,
        --((pr.price_eur * oi.quantity) + (o.shipping_cost_eur / c.item_count)) - (p.discount_eur / c.item_count) AS order_total_by_item,       
        --o.shipping_cost_eur AS shipping_order_cost_eur,        
        --order_cost_eur,
        promo_id,
        --discount_eur AS discount_order_eur,       
        --order_total_eur
        FROM int_sales