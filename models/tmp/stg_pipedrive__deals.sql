select distinct on (id)
    id as deal_id,
    title as deal_name,
    update_time as updated_at_time,
    add_time as added_at_time,
    close_time as closed_at_time,
    deleted as is_deleted,
    active as is_active,
    *
from
    {{ var('deals') }}

