with users as (
    select
        user_id,
        user_name
    from
        {{ ref('stg_pipedrive__users') }}
),

stages as (
    select
        stage_id,
        stage_name
    from
        {{ ref('stg_pipedrive__stages') }}
),

opened_deals as (
    select distinct on (id)
        deal_id,
        deal_name,
        date_trunc('day', add_time) as "date",
        update_time,
        lower(trim(users.user_name)) as user_name,
        lower(trim(stages.stage_name)) as stage_name,
        close_time - add_time as "expected_time_to_close"
    from
        {{ ref('stg_pipedrive__deals') }}
    left join users using (user_id)
    left join stages using (stage_id)
    where status != 'deleted'
order by
    id,
    update_time asc
)

select * from opened_deals
