-- macro with a custom generic test to check positive values in a column
{% test positive_value(model, column_name) %}
SELECT * 
FROM {{ model }} 
WHERE {{ column_name }} < 1
{% endtest %}