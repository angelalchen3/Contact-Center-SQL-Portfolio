-- Purpose:
--   Aggregate inbound agent interactions by 30-minute interval, division, and location.

SELECT
    interaction_timestamp::date                  AS interaction_date,
    time_bucket_30min                            AS time_interval_30min,
    agent_division                                AS agent_division,
    CASE
        WHEN agent_location = 'Location A (raw)' THEN 'Location A'
        WHEN agent_location = 'Location B (raw)' THEN 'Location B'
        ELSE agent_location
    END                                           AS agent_location_clean,
    COUNT(*)                                      AS interaction_count,
    SUM(handle_seconds)                           AS total_handle_seconds,
    SUM(talk_seconds)                             AS total_talk_seconds,
    SUM(hold_seconds)                             AS total_hold_seconds,
    SUM(acw_seconds)                              AS total_acw_seconds,
    SUM(blind_transfer_count)                     AS blind_transfer_count,
    SUM(consult_transfer_count)                   AS consult_transfer_count,
    'Snowflake'                                   AS data_source
FROM analytics.fact_agent_interactions
WHERE direction = 'inbound'
  AND agent_name IS NOT NULL
  -- Remove excluded agent logic for anonymity; keep as an example pattern:
  AND agent_name NOT IN ('Agent A', 'Agent B', 'Agent C')
  -- Queue exclusion generalized
  AND queue_name NOT IN ('Queue A', 'Queue B', 'Queue C')
  AND interaction_timestamp::date >= DATE '2024-01-01'
  AND agent_division = 'Division X'
GROUP BY
    interaction_timestamp::date,
    time_interval_30min,
    agent_division,
    agent_location_clean
ORDER BY
    interaction_date DESC,
    time_interval_30min ASC;
