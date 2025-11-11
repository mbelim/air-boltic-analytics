
select
  cast(order_id as bigint) as order_id, -- Converting to BIGINT for considering the scalability of future data growth
  cast(customer_id as bigint) as customer_id, -- Converting to BIGINT for considering the scalability of future data growth
  cast(trip_id as bigint) as trip_id, -- Converting to BIGINT for considering the scalability of future data growth
  cast(price_eur as decimal(18,2)) as price_eur,
  seat_no,
  status,
  _loaded_at -- Assuming this column exists, see readme for more info under "Key Assumptions & Design Decisions" section
from {{ source('raw_air_boltic', 'order') }}