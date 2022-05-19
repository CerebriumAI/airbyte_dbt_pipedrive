select
    user_id,
    user_name,
    created_at_date,
    is_active
from {{ ref('stg_pipedrive__users') }}
