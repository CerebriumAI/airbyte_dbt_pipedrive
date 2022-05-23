with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

leads as (
    select
        *
    from
        {{ ref('stg_pipedrive__leads') }}
),

leads_final as (
    select distinct on (id)
        leads.lead_id,
        leads.created_at_date,
        leads.updated_at_date,
        leads.is_archived,
        users.user_name,
        leads.owner_id,
        leads.amount,
        leads.currency,
        leads.source_name as "source",
        leads.expected_close_date - leads.add_time as "expected_time_to_close"
    from
        leads
    left join users on user_id = owner_id
order by
    id,
    updated_at_date desc
)

select * from leads_final
