select
    id as stage_id,
    name as stage_name
from
    {{ var('stages') }}
