with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

activities as (
    select distinct on (id)
        id,
        date,
        users.user_name,
        type,
        done
    from
        {{ ref('stg_pipedrive__activities') }}
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

select * from user_activities
