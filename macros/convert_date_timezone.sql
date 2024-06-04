--funcion para convertir timezone 
{% macro convert_date_timezone(column, alias_table_with_point=None, timezone_new='UTC') %}
    {%- if alias_table_with_point %}
        CONVERT_TIMEZONE('{{ timezone_new }}', {{ alias_table_with_point }}{{ column }}) AS {{ column }}_{{ timezone_new }}
    {%- else %}
        CONVERT_TIMEZONE('{{ timezone_new }}', {{ column }}) AS {{ column }}_{{ timezone_new }}
    {%- endif %}
{% endmacro %}