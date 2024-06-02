
WITH int_date AS (
   {{ dbt_date.get_date_dimension("2020-01-01", "2030-12-31") }} 

)

SELECT 
    DATE_DAY AS DATE_ID,
    DAY_OF_WEEK,
    DAY_OF_WEEK_NAME,
    DAY_OF_MONTH,
    DAY_OF_YEAR,
    WEEK_OF_YEAR,
    MONTH_NAME,
    MONTH_OF_YEAR,
    QUARTER_OF_YEAR,
    YEAR_NUMBER   
 FROM int_date