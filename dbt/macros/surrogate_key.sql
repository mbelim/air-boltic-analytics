-- macros/surrogate_key.sql
-- For scalability, we create surrogate keys using an md5 hash of the concatenated columns.
-- This ensures uniqueness and handles large datasets efficiently.
{% macro surrogate_key(cols) %}
  md5(concat_ws('|', {{ cols | join(', ') }}))
{% endmacro %}
