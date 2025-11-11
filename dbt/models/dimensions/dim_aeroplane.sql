{{
    config(
        materialized='table'
    )
}}

select
    {{ surrogate_key(['stg_aeroplane.aeroplane_id']) }} as aeroplane_key,
    stg_aeroplane.aeroplane_id,
    
    stg_aeroplane.manufacturer,
    stg_aeroplane.model_name,
    stg_model.max_seats,
    stg_model.max_weight_kg,
    stg_model.max_distance_nm,
    stg_model.engine_type,
    
    -- === CALCULATED COLUMN (from aeroplane json) ===
    case
        when stg_model.max_seats < 20 then 'Private Jet'
        when stg_model.max_seats < 100 then 'Small Regional'
        when stg_model.max_seats < 250 then 'Medium Haul'
        else 'Large Haul'
    end as plane_size_category

from {{ ref('stg_aeroplane') }} as stg_aeroplane
left join {{ ref('stg_aeroplane_model') }} as stg_model
    on stg_aeroplane.manufacturer = stg_model.manufacturer
    and stg_aeroplane.model_name = stg_model.model_name