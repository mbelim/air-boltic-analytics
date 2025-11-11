{% test is_timestamp(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND NOT (CAST({{ column_name }} AS STRING) RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?$')
{% endtest %}