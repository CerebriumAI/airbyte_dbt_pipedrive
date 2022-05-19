with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

leads as (
    select distinct on (id)
        lead_id,
        _airbyte_leads_hashid,
        date_trunc('day', add_time) as "date",
        update_time,
        is_archived,
        users.user_name,
        owner_id,
        amount,
        currency,
        source_name as "source",
        expected_close_date - add_time as "expected_time_to_close"
    from
        {{ ref('stg_pipedrive__leads') }}
    left join users on user_id = owner_id
order by
    id,
    update_time desc
)

select * from leads
