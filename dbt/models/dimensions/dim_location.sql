{{
    config(
        materialized='table'
    )
}}

with all_cities as (
    select distinct origin_city as city from {{ ref('stg_trip') }}
    union
    select distinct destination_city as city from {{ ref('stg_trip') }}
)
select
    {{ surrogate_key(['city']) }} as location_key,
    city,
    -- Assuming these columns would be filled in by joining to an external source (e.g., Geonames)
    null as country,
    null as region,
    null as continent
from all_cities