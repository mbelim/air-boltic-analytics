{{
    config(
        materialized='table'
    )
}}

select
    {{ surrogate_key(['stg_trip.trip_id']) }} as trip_key,
    stg_trip.trip_id,

    -- Foreign keys to other dimensions
    dim_plane.aeroplane_key,
    dim_origin.location_key as origin_location_key,
    dim_dest.location_key as destination_location_key,
    
    stg_trip.start_timestamp,
    stg_trip.end_timestamp,
    
    -- === CALCULATED COLUMNS ===
    timestampdiff(HOUR, stg_trip.start_timestamp, stg_trip.end_timestamp) as duration_hours,
    
    case
        when timestampdiff(HOUR, stg_trip.start_timestamp, stg_trip.end_timestamp) < 3 then 'Short Haul'
        when timestampdiff(HOUR, stg_trip.start_timestamp, stg_trip.end_timestamp) < 8 then 'Medium Haul'
        else 'Long Haul'
    end as trip_distance_category

from {{ ref('stg_trip') }} as stg_trip

left join {{ ref('dim_aeroplane') }} as dim_plane
    on stg_trip.aeroplane_id = dim_plane.aeroplane_id
left join {{ ref('dim_location') }} as dim_origin
    on stg_trip.origin_city = dim_origin.city
left join {{ ref('dim_location') }} as dim_dest
    on stg_trip.destination_city = dim_dest.city