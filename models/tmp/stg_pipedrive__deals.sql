select distinct on (id)
    id as deal_id,
    title as deal_name,
    date_trunc('day', add_time) as "created_at_date",
    date_trunc('day', update_time) as "updated_at_date",
    date_trunc('day', close_time) as "closed_at_date",
    deleted as is_deleted,
    active as is_active,
    *
from
    {{ var('deals') }}

