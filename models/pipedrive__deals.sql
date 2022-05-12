WITH agents AS (
    SELECT
        id AS "user_id",
        name "agent_name"
    FROM
        {{ var('users') }}
),

stages AS (
    SELECT
        id AS "stage_id",
        name as "stage_name"
    FROM
        {{ var('stages') }}
),

deals AS (
    SELECT DISTINCT ON (id)
        id as deal_id,
        title as deal_name,
        org_id,
        date_trunc('day', add_time) AS "date",
        date_trunc('day', close_time) AS "close_date",
        LOWER(TRIM(agents.agent_name)) as agent_name,
        LOWER(TRIM(stages.stage_name)) as stage_name,
        LOWER(TRIM(status)) as deal_status,
        stage_id,
        value::NUMERIC as amount,
        UPPER(TRIM(currency)) as currency,
        close_time - add_time AS "time_to_close"
    FROM
        {{ var('deals') }}
    LEFT JOIN agents USING (user_id)
    LEFT JOIN stages USING (stage_id)
    WHERE status != 'deleted'
ORDER BY
    id,
    update_time DESC
),

deals_final AS (
    SELECT
        *
    FROM
        deals
)

SELECT
    *
FROM
    deals_final
