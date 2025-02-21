{% macro debug_log(message) %}
  {% if execute %}
    {{ log(message, info=True) }}
  {% endif %}
{% endmacro %}