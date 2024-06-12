--TABLA ORDERS-ORDERS_ITEMS UNIFICADA  CON TODO( UNION ORDERS - ORDERS_ITEM - PRODUCTS - PROMOS)
{{
  config(
    materialized='view'
  )
}}


WITH count_order_items AS(
SELECT order_id, COUNT(*) as item_count
    FROM {{ ref('stg_sql_server_dbo__order_items') }}
    GROUP BY order_id
), 

final_table AS (
SELECT  
        o.user_id,
        o.order_id,
        o.address_id,
        DATE(o.created_at_utc) AS created_at_utc,
        LEFT(DATE(o.created_at_utc),7) AS year_month_created_at_utc,
        DATE(o.delivered_at_utc) AS delivered_at_utc,
        oi.product_id,
        oi.quantity,
        pr.price_eur AS price_product_eur,
        pr.price_eur * oi.quantity AS order_cost_by_item_eur,
        o.shipping_cost_eur / c.item_count AS shipping_cost_by_item_eur,        
        (pr.price_eur * oi.quantity) + (o.shipping_cost_eur/c.item_count) AS order_cost_and_shipping_by_item_eur,
        p.discount_eur / c.item_count AS discount_by_item_eur,
        ((pr.price_eur * oi.quantity) + (o.shipping_cost_eur / c.item_count)) - (p.discount_eur / c.item_count) AS order_total_by_item,       
        o.shipping_cost_eur AS shipping_order_cost_eur,        
        o.order_cost_eur AS order_cost_eur,
        o.promo_id,
        p.discount_eur AS discount_order_eur,       
        o.order_total_eur 
FROM
{{ ref('stg_sql_server_dbo__orders') }} o
JOIN {{ ref('stg_sql_server_dbo__order_items') }} oi
ON o.order_id = oi.order_id
JOIN {{ ref('stg_sql_server_dbo__promos') }} p
ON o.promo_id = p.promo_id
JOIN {{ ref('stg_sql_server_dbo__products') }} pr
ON pr.product_id = oi.product_id
JOIN count_order_items c
ON c.order_id = oi.order_id
)

SELECT * FROM final_table