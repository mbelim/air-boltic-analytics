{% macro test_incremental_not_null(model, column_name, timestamp_column, time_window_hours=24) %}
{#
    Tests for NOT NULL on a column, but only for records loaded within the specified time window.
    This prevents a full-table scan on every run.
#}
select
    *
from {{ model }}
where
    {{ column_name }} is null
    and {{ timestamp_column }} >= current_timestamp() - interval '{{ time_window_hours }} hours'

{% endmacro %}