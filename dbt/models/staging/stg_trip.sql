
select
  cast(trip_id as bigint) as trip_id, -- Converting to BIGINT for considering the scalability of future data growth
  origin_city,
  destination_city,
  cast(airplane_id as string) as airplane_id, -- Converting to BIGINT for considering the scalability of future data growth
  cast(start_timestamp as timestamp) as start_timestamp,
  cast(end_timestamp as timestamp) as end_timestamp,
  _loaded_at -- Assuming this column exists, see readme for more info under "Key Assumptions & Design Decisions" section
from {{ source('raw_air_boltic', 'trip') }}