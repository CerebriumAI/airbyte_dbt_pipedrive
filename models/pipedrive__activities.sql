with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

activities as (
    select
        *
    from
        {{ ref('stg_pipedrive__activities') }}
),

activities_final as (
    select distinct on (activity_id)
        activities.activity_id,
        activities.updated_at_date,
        users.user_id,
        users.user_name,
        activities.type,
        activities.is_done
    from
        activities
    left join users on assigned_to_user_id = users.user_id
    order by
        activity_id,
        updated_at_date desc
)

select * from activities_final
