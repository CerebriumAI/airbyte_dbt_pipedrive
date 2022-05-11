select
    id as user_id,
    *
from {{ var('users') }}
