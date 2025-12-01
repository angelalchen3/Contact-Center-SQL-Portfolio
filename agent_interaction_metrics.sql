-- Purpose:
--   1) Daily agent-level interaction metrics with time-in-state breakdown
--   2) Monthly call volume by queue

-------------------------------
-- 1. Daily agent metrics
-------------------------------

SELECT
    interaction_date::date              AS interaction_date,
    agent_name                          AS agent_name,
    agent_division                      AS agent_division,
    MIN(team_name)                      AS team_name,
    COUNT(*)                            AS agent_conversation_count,
    SUM(handle_seconds)                 AS handle_seconds,
    SUM(talk_seconds)                   AS talk_seconds,
    SUM(hold_seconds)                   AS hold_seconds,
    SUM(acw_seconds)                    AS acw_seconds,
    SUM(blind_transfer_count)           AS blind_transfer_count,
    SUM(consult_transfer_count)         AS consult_transfer_count,
    CASE
        WHEN agent_location = 'Location A (raw)'
            THEN 'Location A'
        WHEN agent_location = 'Location B (raw)'
            THEN 'Location B'
        ELSE agent_location
    END                                 AS agent_location_clean,
    'Snowflake'                         AS data_source
FROM analytics.fact_agent_interactions
WHERE interaction_date::date >= DATE '2024-01-01'
GROUP BY
    interaction_date::date,
    agent_name,
    agent_division,
    CASE
        WHEN agent_location = 'Location A (raw)'
            THEN 'Location A'
        WHEN agent_location = 'Location B (raw)'
            THEN 'Location B'
        ELSE agent_location
    END
ORDER BY
    interaction_date ASC,
    agent_name ASC;


-------------------------------
-- 2. Monthly call volume by queue
-------------------------------

SELECT
    DATE_TRUNC('month', interaction_date) AS month_start,
    queue_name                            AS queue_name,
    COUNT(*)                              AS call_count
FROM analytics.fact_agent_interactions
WHERE DATE_TRUNC('month', interaction_date) = DATE '2024-08-01'
GROUP BY
    month_start,
    queue_name
ORDER BY
    call_count DESC;
