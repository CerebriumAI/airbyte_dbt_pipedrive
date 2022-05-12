select
    id as user_id,
    name as user_name
from
    {{ var('users') }}
