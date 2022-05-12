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

opened_deals AS (
    SELECT DISTINCT ON (id)
        id as deal_id,
        title as deal_name,
        date_trunc('day', add_time) AS "date",
        update_time,
        LOWER(TRIM(agents.agent_name)) as agent_name,
        LOWER(TRIM(stages.stage_name)) as stage_name,
        close_time - add_time AS "expected_time_to_close"
    FROM
        {{ var('deals') }}
    LEFT JOIN agents USING (user_id)
    LEFT JOIN stages USING (stage_id)
    WHERE status != 'deleted'
ORDER BY
    id,
    update_time ASC
)
SELECT
    *
FROM
    opened_deals
