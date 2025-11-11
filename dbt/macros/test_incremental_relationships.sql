{% macro test_incremental_relationships(model, column_name, to, field, timestamp_column, time_window_hours=24) %}
{#
    Tests for referential integrity, but only for incrementally loaded records.
#}
select
    child.{{ column_name }}
from {{ model }} as child
left join {{ to }} as parent
    on child.{{ column_name }} = parent.{{ field }}
where
    -- The FK link is broken
    parent.{{ field }} is null
    
    -- (the incremental part)
    and child.{{ column_name }} is not null
    and child.{{ timestamp_column }} >= current_timestamp() - interval '{{ time_window_hours }} hours'

{% endmacro %}