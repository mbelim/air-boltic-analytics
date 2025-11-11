
select
  cast(id as bigint)  as customer_group_id, -- Converting to BIGINT for considering the scalability of future data growth
  type as customer_group_type,
  name as customer_group_name,
  registry_number,
  _loaded_at -- Assuming this column exists, see readme for more info under "Key Assumptions & Design Decisions" section
from {{ source('raw_air_boltic', 'customer_group') }}
