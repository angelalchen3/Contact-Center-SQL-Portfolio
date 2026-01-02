-- ivr_event_summary_30min.sql
-- Purpose:
--   Summarize self-service events by 30-minute interval, including
--   resolution (contained) and escalation (transferred) behavior.

WITH parameters AS (
  SELECT
    TO_DATE('2024-01-01') AS start_date
)

SELECT
    event_ts::date                                           AS event_date,
    interval_30min                                            AS interval_30min,
    COUNT(*)                                                  AS event_count,
    SUM(CASE WHEN resolved_flag = 1 THEN 1 ELSE 0 END)        AS resolved_count,
    SUM(CASE WHEN escalated_flag = 1 THEN 1 ELSE 0 END)       AS escalated_count,
    SUM(duration_seconds)                                     AS total_duration_seconds,
    SUM(CASE WHEN resolved_flag = 1 THEN duration_seconds END)     AS resolved_duration_seconds,
    SUM(CASE WHEN resolved_flag = 0 THEN duration_seconds END)     AS unresolved_duration_seconds,
    'example'                                                 AS data_source
FROM analytics.fact_self_service_events e
CROSS JOIN parameters p
WHERE e.event_ts::date >= p.start_date
  AND interval_30min IS NOT NULL
GROUP BY
    event_ts::date,
    interval_30min
ORDER BY
    event_date DESC,
    interval_30min;
