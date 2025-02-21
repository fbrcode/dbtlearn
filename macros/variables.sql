{% macro variables() %}
  {# jinja variable #}
  {% set your_name_jinja = "Fabio" %}
  {{ log("Hello " ~ your_name_jinja, info=True) }}

  {# dbt variable - can be set with --vars on command line or come from dbt_project.yml #}
  {{ log("Hello dbt user: " ~ var("user_name", "<NO USERNAME DEFINED>"), info=True) }}
{% endmacro %}