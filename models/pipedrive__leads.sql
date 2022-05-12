WITH agents AS (
    SELECT
        id AS "owner_id",
        name "agent_name"
    FROM
        {{ var('users') }}
),

lead_value AS (
    SELECT
        _airbyte_leads_hashid,
        amount,
        currency
    FROM
        {{ var('leads_value') }}
),

leads AS (
    SELECT DISTINCT ON (id)
        id as lead_id,
        _airbyte_leads_hashid,
        date_trunc('day', add_time) AS "date",
        update_time,
        is_archived,
        agents.agent_name,
        owner_id,
        lead_value.amount as amount,
        lead_value.currency as currency,
        source_name AS "source",
        expected_close_date - add_time AS "expected_time_to_close"
    FROM
        {{ var('leads') }}
    LEFT JOIN agents USING (owner_id)
    LEFT JOIN lead_value USING (_airbyte_leads_hashid)
ORDER BY
    id,
    update_time DESC
)

SELECT
    date,
    LOWER(TRIM(agent_name)) as agent_name,
    LOWER(TRIM(source)) as source,
    COUNT(*) AS "total_leads",
    COALESCE(SUM(amount), 0) AS "total_lead_value",
    COALESCE(ROUND(AVG(amount), 2), 0) AS "avg_lead_value",
    COALESCE(AVG(expected_time_to_close), INTERVAL '0 hours') AS "avg_expected_time_to_close"
FROM
    leads
GROUP BY
    date,
    agent_name,
    source
