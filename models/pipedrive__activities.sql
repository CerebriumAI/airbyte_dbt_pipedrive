WITH agents AS (
    SELECT
        id AS "owner_id",
        name "agent_name"
    FROM
        {{ var('users') }}
),

activities AS (
    SELECT DISTINCT ON (id)
        id,
        date_trunc('day', date(update_time)) AS "date",
        agents.agent_name,
        type,
        done
    FROM
        {{ var('activities') }}
    LEFT JOIN agents on assigned_to_user_id = agents.owner_id
ORDER BY
    id,
    date DESC
),

agent_activities AS (
SELECT
    date,
    LOWER(TRIM(agent_name)) as agent_name,
    LOWER(TRIM(type)) as type,
    COUNT(*) AS "total_activities",
    COUNT(CASE WHEN (done = TRUE) THEN 1 END) as "completed_activities"

FROM
    activities
GROUP BY
    date,
    agent_name,
    type
)
SELECT
    *
FROM
    agent_activities
