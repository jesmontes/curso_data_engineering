WITH sales_budget AS (
        SELECT  YEAR_MONTH,
                product_id,
                SUM(quantity) AS estimated_quantity
        from {{ref("stg_google_sheets__budget")}} b
       
        GROUP BY 1,2
        ),

 int_ord_ord_it AS (
      SELECT
            LEFT(created_at_utc,7) AS YEAR_MONTH,            
            product_id,
            price_product_eur,
            SUM(quantity) as quantity                   
            from {{ref("int_orders_order_items_grouped")}}
            group by 1,2,3             
 ) ,
 
joined AS (
 SELECT a.YEAR_MONTH
        ,a.product_id
        ,a.estimated_quantity AS target_quantity
        ,b.quantity AS quantity_sold
        ,b.quantity - a.estimated_quantity AS variance
        ,b.quantity * b.price_product_eur AS sales_revenue_eur --ingresos por ventas
        ,a.estimated_quantity * b.price_product_eur AS target_sales_revenue_eur 
        ,ROUND((b.quantity / a.estimated_quantity)* 100,0) AS Target_Achievement_Rate --porcentaje de la cantidad vendida con respecto a la cantidad objetivo,
        FROM sales_budget a
        LEFT JOIN int_ord_ord_it b
        ON a.YEAR_MONTH = b.YEAR_MONTH AND a.product_id = b.product_id          
)

SELECT * FROM joined 