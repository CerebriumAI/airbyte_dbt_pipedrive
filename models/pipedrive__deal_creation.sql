with users as (
    select
        id as "user_id",
        name "user_name"
    from
        {{ var('users') }}
),

stages as (
    select
        id as "stage_id",
        name as "stage_name"
    from
        {{ var('stages') }}
),

opened_deals as (
    select distinct on (id)
        id as deal_id,
        title as deal_name,
        date_trunc('day', add_time) as "date",
        update_time,
        lower(trim(users.user_name)) as user_name,
        lower(trim(stages.stage_name)) as stage_name,
        close_time - add_time as "expected_time_to_close"
    from
        {{ var('deals') }}
    left join users using (user_id)
    left join stages using (stage_id)
    where status != 'deleted'
order by
    id,
    update_time asc
)
select
    *
from
    opened_deals
