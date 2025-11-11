
select
  cast(customer_id as bigint)  as customer_id,  -- Converting to BIGINT for considering the scalability of future data growth
  name AS customer_name,
  cast(customer_group_id as bigint) as customer_group_id, -- Converting to BIGINT for considering the scalability of future data growth
  email,
  phone_number,
  _loaded_at -- Assuming this column exists, see readme for more info under "Key Assumptions & Design Decisions" section
from {{ source('raw_air_boltic', 'customer') }}