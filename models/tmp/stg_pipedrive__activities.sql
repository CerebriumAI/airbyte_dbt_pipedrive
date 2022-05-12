with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

activities as (
    select distinct on (id)
        id,
        date_trunc('day', date(update_time)) as "date",
        users.user_name,
        type,
        done
    from
        {{ var('activities') }}
    left join users on assigned_to_user_id = users.user_id
order by
    id,
    date desc
),

user_activities as (
    select
        date,
        lower(trim(user_name)) as user_name,
        lower(trim(type)) as type,
        count(*) as "total_activities",
        count(case when (done = true) then 1 end) as "completed_activities"

    from
        activities
    group by
        date,
        user_name,
        type
)

select
    *
from
    user_activities
