--Poner primera letra en mayuscula
{%macro upper_first_letter (column)%}
CONCAT(UPPER(SUBSTRING({{column}}, 1, 1)), LOWER(SUBSTRING({{column}}, 2)))

{%endmacro%}