with lead_value as (
    select
        _airbyte_leads_hashid,
        amount,
        currency
    from
        {{ var('leads_value') }}
),

leads as (
    select
        id as lead_id,
        date_trunc('day', add_time) as "created_at_date",
        date_trunc('day', update_time) as "updated_at_date",
        *
    from
        {{ var('leads') }}
    left join lead_value using (_airbyte_leads_hashid)
)

select * from leads
