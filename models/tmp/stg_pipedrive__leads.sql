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
        *
    from
        {{ var('leads') }}
    left join lead_value using (_airbyte_leads_hashid)
)

select * from leads
