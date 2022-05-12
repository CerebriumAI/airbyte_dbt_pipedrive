select distinct on (id)
    id as deal_id,
    *
from
    {{ var('deals') }}

