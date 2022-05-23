select
    id as user_id,
    name as user_name,
    created as created_at_date,
    active_flag as is_active,
    is_admin
from
    {{ var('users') }}
