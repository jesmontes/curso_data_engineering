WITH budget AS (
    SELECT  
        b.month AS date_day,
        b.product_id,
        SUM(b.quantity) AS estimated_quantity
    FROM 
        {{ref('stg_google_sheets__budget')}} b
    GROUP BY 
        b.month, b.product_id
),
orders AS (
    SELECT
        LEFT(a.created_at_utc, 10) AS date_day,  
        a.product_id,
        SUM(a.quantity) as quantity
    FROM 
        {{ref('int_orders_order_items_grouped')}} a
    GROUP BY 
        LEFT(a.created_at_utc, 10), a.product_id
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
SELECT  YEAR(date_day) as anio,
        MONTH(date_day) as mes,
        product_id,
        SUM(target_quantity),
        SUM(quantity_sold)
FROM joined
GROUP BY all
ORDER BY 1,2,3
