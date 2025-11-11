{{
    config(
        materialized='table'
    )
}}

-- This uses a macro to build a complete date dimension, 
-- which is far more reliable than manual SQL.
select *
from {{ dbt_utils.date_spine(
    date_part=day,
    start_date=cast('2020-01-01' as date),
    end_date=cast('2031-01-01' as date)
   )
}}