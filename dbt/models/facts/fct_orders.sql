{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        -- For Databricks, searched below criteria on LM model
        cluster_by = ['order_date', 'customer_key']
    )
}}

select
    -- Keys
    stg_order.order_id,
    dim_trip.trip_key,
    dim_cust.customer_key,
    
    -- Date key. CRITICAL ASSUMPTION:
    -- Using the trip's start date as the order date.
    cast(dim_trip.start_timestamp as date) as order_date,

    -- dimensions
    stg_order.seat_no,
    stg_order.status,
    
    -- Measures
    stg_order.price_eur,
    1 as num_orders,

    -- === CALCULATED COLUMN  ===
    case
        when stg_order.price_eur < 500 then 'Low Cost'
        when stg_order.price_eur < 1500 then 'Standard'
        else 'Premium'
    end as price_category,

    stg_order._loaded_at
    
from {{ ref('stg_order') }} as stg_order

left join {{ ref('dim_trip') }} as dim_trip
    on stg_order.trip_id = dim_trip.trip_id
left join {{ ref('dim_customer') }} as dim_cust
    on stg_order.customer_id = dim_cust.customer_id
left join {{ ref('dim_date') }} as dim_date
    on cast(dim_trip.start_timestamp as date) = dim_date.date_day

{% if is_incremental() %}
    -- Assumes the `_loaded_at` column is available from the stg_order model
    where stg_order._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}