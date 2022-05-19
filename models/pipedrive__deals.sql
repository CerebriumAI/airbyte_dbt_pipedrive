with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

stages as (
    select
        *
    from
        {{ ref('stg_pipedrive__stages') }}
),

deals as (
    select
        *
    from
        {{ ref('stg_pipedrive__deals') }}
),

deals_final as (
    select
        deals.added_at_time,
        deals.closed_at_time,
        deals.currency,
        deals.deal_id,
        deals.deal_name,
        deals.is_active,
        deals.is_deleted,
        deals.updated_at_time,
        deals.weighted_value,
        stages.stage_name,
        users.user_name
    from
        deals
    left join users using (user_id)
    left join stages using (stage_id)
    where status != 'deleted'
order by
    id,
    update_time desc
)

select * from deals_final
