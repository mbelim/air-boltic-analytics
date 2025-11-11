{{
    config(
        materialized='table'
    )
}}

select
    {{ surrogate_key(['stg_cust.customer_id']) }} as customer_key,
    stg_cust.customer_id,
    stg_cust.customer_name,
    stg_cust.email,
    stg_cust.phone_number,
    
    stg_group.customer_group_type,
    stg_group.customer_group_name,
    stg_group.registry_number,

    -- === CALCULATED COLUMN ===
    coalesce(stg_group.customer_group_type, 'Individual') as customer_type
    
from {{ ref('stg_customer') }} as stg_cust
left join {{ ref('stg_customer_group') }} as stg_group
    on stg_cust.customer_group_id = stg_group.customer_group_id