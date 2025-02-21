-- this macro check if any column in a model has NULL values
{% macro no_nulls_in_column(model) %}
  SELECT * FROM {{ model }} WHERE 
  {% for col in adapter.get_columns_in_relation(model) -%} -- hyphen trim the whitespaces (single line expression)
    {{ col.column }} IS NULL OR
  {% endfor %}
  FALSE
{% endmacro %}
