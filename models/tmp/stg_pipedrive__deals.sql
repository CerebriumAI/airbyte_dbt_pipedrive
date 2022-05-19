select distinct on (id)
    id as deal_id,
    title as deal_name,
    *
from
    {{ var('deals') }}

