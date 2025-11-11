
select
  cast(airplane_id as bigint) as airplane_id, -- Converting to BIGINT for considering the scalability of future data growth
  airplane_model,
  manufacturer,
  _loaded_at -- Assuming this column exists, see readme for more info under "Key Assumptions & Design Decisions" section
from {{ source('raw_air_boltic', 'aeroplane') }}
