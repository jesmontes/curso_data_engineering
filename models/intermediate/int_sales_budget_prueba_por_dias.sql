WITH budget AS (
    SELECT  
        d.date_day,
        b.product_id,
        SUM(b.quantity) AS estimated_quantity
    FROM 
        {{ref('stg_google_sheets__budget')}}  b
    JOIN 
        {{ref('dim_date')}} d
    ON 
        b.month = d.date_day
    GROUP BY 
        d.date_day, b.product_id
),
orders AS (
    SELECT
        b.date_day,
        a.product_id,
        a.price_product_eur,
        SUM(a.quantity) as quantity                   
    FROM 
        {{ref('int_orders_order_items_grouped')}}  a
    JOIN 
         {{ref('dim_date')}}  b
    ON 
        a.created_at_utc = b.date_day
    GROUP BY 
        b.date_day, a.product_id, a.price_product_eur
),
joined AS (
    SELECT  
        COALESCE(a.date_day, b.date_day) AS date_day,
        COALESCE(a.product_id, b.product_id) AS product_id,
        a.estimated_quantity AS target_quantity,
        b.quantity AS quantity_sold
    FROM 
        budget a
    FULL OUTER JOIN 
        orders b
    ON  
        a.product_id = b.product_id AND
        a.date_day = b.date_day
)
SELECT * 
FROM joined
ORDER BY date_day, product_id
