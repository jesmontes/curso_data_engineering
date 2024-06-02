--funcion para convertir timezone 
{% macro convert_date_timezone(column, timezone_new ='UTC') %}

CONVERT_TIMEZONE('{{timezone_new}}', {{column}}) AS {{column}}_{{timezone_new}}
{% endmacro%}