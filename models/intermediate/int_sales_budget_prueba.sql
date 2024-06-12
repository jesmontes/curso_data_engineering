WITH budget AS( 
    SELECT  date_day,
        product_id,
        SUM(quantity) AS estimated_quantity
    FROM {{ref('stg_google_sheets__budget')}} b
    JOIN {{ref('dim_date')}} d
    ON b.month = d.date_day
    GROUP BY 1,2
    ),
orders AS (
   SELECT
            --LEFT(created_at_utc,7) AS YEAR_MONTH,                        
            date_day,
            YEAR_MONTH,
            product_id,
            price_product_eur,
            SUM(quantity) as quantity                   
            from {{ref('int_orders_order_items_grouped')}} a
            JOIN {{ref('dim_date')}}  b
            ON a.created_at_utc = b.date_day
            group by 1,2,3,4 
        
        ),
joined AS (
 SELECT a.date_day
        ,b.YEAR_MONTH
        ,a.product_id
        ,a.estimated_quantity AS target_quantity
        ,SUM(b.quantity) AS quantity_sold
        --,b.quantity - a.estimated_quantity AS variance
        --,b.quantity * b.price_product_eur AS sales_revenue_eur --ingresos por ventas
        --,a.estimated_quantity * b.price_product_eur AS target_sales_revenue_eur 
        --,ROUND((b.quantity / a.estimated_quantity)* 100,0) AS Target_Achievement_Rate --porcentaje de la cantidad vendida con respecto a la cantidad objetivo,
        FROM budget a
        LEFT JOIN orders b
        ON  MONTH(a.date_day) = MONTH(b.date_day)  AND a.product_id = b.product_id 
        GROUP BY 1,2,3,4
)
SELECT * FROM joined order by date_day, product_id