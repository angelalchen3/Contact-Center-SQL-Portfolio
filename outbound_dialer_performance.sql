-- campaign_event_summary.sql
-- Purpose:
--   Summarize campaign event volume, successful outcomes, and time-based metrics
--   grouped by date and a hashed entity bucket (clean-room portfolio example).

WITH parameters AS (
  SELECT
    TO_DATE('2024-08-01') AS start_date,
    100                   AS bucket_count
),

load_events AS (
  SELECT
    CAST(load_ts AS DATE) AS load_date,
    SUM(records_loaded)   AS records_loaded
  FROM analytics.fact_load_events
  WHERE load_ts >= (SELECT start_date FROM parameters)
  GROUP BY 1
),

base_events AS (
  SELECT
    CAST(e.event_end_ts AS DATE)                              AS event_date,
    e.entity_id                                               AS entity_id,
    e.event_outcome                                           AS event_outcome,
    e.event_type                                              AS event_type,
    e.event_end_ts                                            AS event_end_ts,
    e.metric_work_seconds                                     AS work_seconds,
    e.metric_active_seconds                                   AS active_seconds,
    e.metric_wait_seconds                                     AS wait_seconds,
    e.metric_hold_seconds                                     AS hold_seconds,
    e.metric_wrap_seconds                                     AS wrap_seconds,
    MOD(ABS(HASH(e.entity_id)), (SELECT bucket_count FROM parameters)) AS entity_bucket
  FROM analytics.fact_campaign_events e
  WHERE e.event_end_ts >= (SELECT start_date FROM parameters)
),

attempts AS (
  SELECT
    b.event_date,
    b.entity_bucket,
    COUNT(*)                     AS attempt_count,
    COUNT(DISTINCT b.entity_id)  AS unique_entities_attempted,
    AVG(le.records_loaded)       AS avg_records_loaded
  FROM base_events b
  LEFT JOIN load_events le
    ON le.load_date = b.event_date
  WHERE b.event_outcome IN ('SUCCESS', 'FAILURE', 'NO_RESPONSE', 'SYSTEM')
  GROUP BY 1,2
),

successes AS (
  SELECT
    b.event_date,
    b.entity_bucket,
    COUNT(*)                                              AS success_count,
    SUM(CASE WHEN b.event_type = 'TARGET_ACTION' THEN 1 ELSE 0 END) AS target_action_count,
    SUM(b.active_seconds)                                 AS total_active_seconds,
    SUM(b.hold_seconds)                                   AS total_hold_seconds,
    SUM(b.wrap_seconds)                                   AS total_wrap_seconds
  FROM base_events b
  WHERE b.event_outcome = 'SUCCESS'
  GROUP BY 1,2
)

SELECT
  a.event_date,
  a.entity_bucket,
  a.attempt_count,
  a.unique_entities_attempted,
  a.avg_records_loaded,
  COALESCE(s.success_count, 0)         AS success_count,
  COALESCE(s.target_action_count, 0)   AS target_action_count,
  s.total_active_seconds,
  s.total_hold_seconds,
  s.total_wrap_seconds
FROM attempts a
LEFT JOIN successes s
  ON s.event_date = a.event_date
 AND s.entity_bucket = a.entity_bucket
ORDER BY a.event_date DESC, a.entity_bucket ASC;
