with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

activities as (
    select distinct on (activity_id)
        activity_id,
        updated_at_date,
        users.user_name,
        type,
        is_done
    from
        {{ ref('stg_pipedrive__activities') }}
    left join users on assigned_to_user_id = users.user_id
    order by
        activity_id,
        updated_at_date desc
),

user_activities as (
    select
        updated_at_date,
        lower(trim(user_name)) as user_name,
        lower(trim(type)) as type,
        count(*) as "total_activities",
        count(case when (is_done = true) then 1 end) as "completed_activities"

    from
        activities
    group by
        updated_at_date,
        user_name,
        type
)

select * from user_activities
