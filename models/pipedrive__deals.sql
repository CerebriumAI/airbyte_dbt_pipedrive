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
        deals.created_at_date,
        deals.updated_at_date,
        deals.closed_at_date,
        deals.currency,
        deals.deal_id,
        deals.deal_name,
        deals.is_active,
        deals.is_deleted,
        deals.weighted_value,
        stages.stage_name,
        stages.stage_id,
        users.user_name,
        users.user_id
    from
        deals
    left join users using (user_id)
    left join stages using (stage_id)
    where status != 'deleted'
order by
    id,
    updated_at_date desc
)

select * from deals_final
