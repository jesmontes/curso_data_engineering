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
            SUM(quantity) as quantity                   
            from {{ref("int_orders_order_items_grouped")}}
            group by 1,2               
 ) ,
 products AS (
    SELECT  product_id,
            price_eur
            FROM {{ref('stg_sql_server_dbo__products')}}
 ),

joined AS (
 SELECT a.YEAR_MONTH,
        a.product_id,
        a.estimated_quantity AS target_quantity,
        b.quantity AS quantity_sold,
        b.quantity - a.estimated_quantity AS variance
        FROM sales_budget a
        LEFT JOIN int_ord_ord_it b
        ON a.YEAR_MONTH = b.YEAR_MONTH AND a.product_id = b.product_id              
)

SELECT * FROM joined 