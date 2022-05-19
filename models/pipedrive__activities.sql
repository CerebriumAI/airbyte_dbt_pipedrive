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
)

select * from activities
