{% macro test_incremental_unique(model, column_name, timestamp_column, time_window_hours=24) %}
{#
    Tests for uniqueness on a column, but only for records loaded within the specified time window.
    This prevents a full-table scan on every run.
#}
select
    {{ column_name }},
    count(*) as n_records
from {{ model }}
where
    -- This is the incremental part
    {{ timestamp_column }} >= current_timestamp() - interval '{{ time_window_hours }} hours'
group by
    {{ column_name }}
having
    count(*) > 1

{% endmacro %}