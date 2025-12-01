-- ivr_interaction_summary.sql
-- Purpose:
--   Summarize IVR interactions by 30-minute interval, including containment
--   and transfer behavior.

SELECT
    ivr_timestamp::date                              AS interaction_date,
    interval_30min                                   AS interval_30min,
    COUNT(*)                                         AS ivr_interaction_count,
    SUM(is_contained)                                AS contained_count,
    SUM(is_transferred)                              AS transferred_count,
    SUM(ivr_duration_seconds)                        AS total_ivr_duration,
    SUM(CASE WHEN is_contained = 1 
             THEN ivr_duration_seconds END)          AS contained_duration,
    SUM(CASE WHEN is_contained = 0 
             THEN ivr_duration_seconds END)          AS uncontained_duration,
    'Snowflake'                                      AS data_source
FROM analytics.fact_ivr_journey
WHERE ivr_timestamp::date >= DATE '2024-01-01'
  AND interval_30min <> 'N/A'
GROUP BY
    ivr_timestamp::date,
    interval_30min
ORDER BY
    interaction_date DESC,
    interval_30min;
