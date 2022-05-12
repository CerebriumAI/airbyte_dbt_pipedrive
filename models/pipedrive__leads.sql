with users as (
    select
        *
    from
        {{ ref('stg_pipedrive__users') }}
),

leads as (
    select distinct on (id)
        id as lead_id,
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

select
    date,
    lower(trim(user_name)) as user_name,
    lower(trim(source)) as source,
    count(*) as "total_leads",
    coalesce(sum(amount), 0) as "total_lead_value",
    coalesce(round(avg(amount), 2), 0) as "avg_lead_value",
    coalesce(avg(expected_time_to_close), interval '0 hours') as "avg_expected_time_to_close"
from
    leads
group by
    date,
    user_name,
    source
