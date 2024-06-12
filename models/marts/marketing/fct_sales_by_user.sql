WITH int_orders AS (

        SELECT 
            user_id,
            MIN(created_at_utc) AS created_at_utc,
            MAX(delivered_at_utc) AS delivered_at_utc,            
            COUNT(DISTINCT(order_id)) AS total_number_orders,       
            SUM(quantity) AS total_quantity_products,
            SUM(order_cost_and_shipping_by_item_eur) AS total_orders_cost_eur,
            SUM(shipping_cost_by_item_eur) AS total_shipping_cost_eur,        
            SUM(discount_by_item_eur) AS total_discount_eur,
            COUNT(DISTINCT(product_id)) AS total_dif_products 
        FROM {{ref('int_orders_order_items_grouped')}}
        GROUP BY 1
),

    users AS (
        SELECT 
            user_id,
            address_id,
            first_name,
            last_name,
            email,
            phone_number
         FROM {{ref('dim_users')}}
    ),

    address AS (
        SELECT 
            address_id,
            address,
            zipcode,
            state,
            country
        FROM {{ref('dim_address')}}
    ),

    joined AS (
        SELECT
                a.user_id,
                b.first_name,
                b.last_name,
                b.email,
                b.phone_number,
                a.created_at_utc,
                a.delivered_at_utc,
                c.address,
                c.zipcode,
                c.state,
                c.country,
                a.total_number_orders,       
                a.total_quantity_products,
                a.total_orders_cost_eur,
                a.total_shipping_cost_eur,        
                a.total_discount_eur,
                a.total_dif_products            
            FROM int_orders a
            JOIN users b
            ON a.user_id = b.user_id
            JOIN address c
            ON b.address_id = c.address_id
            --GROUP BY 1
    )

SELECT * FROM joined    