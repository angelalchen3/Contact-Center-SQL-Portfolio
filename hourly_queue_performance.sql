-- hourly_queue_performance.sql
-- Purpose:
--   Aggregate hourly queue performance metrics and transfer activity
--   using parameterized filters and excluded queues.

WITH parameters AS (
  SELECT
    TO_DATE('2024-01-01') AS start_date,
    'inbound'             AS direction_filter,
    30                    AS quick_abandon_threshold_seconds
),
excluded_queues AS (
  SELECT COLUMN1 AS queue_name
  FROM VALUES ('Example Queue A'), ('Example Queue B')
),
sla_config AS (
  SELECT * FROM VALUES
    ('Tier_1', 90),
    ('Tier_2', 30)
  AS t(queue_tier, sla_seconds)
),
queue_dim AS (
  SELECT * FROM VALUES
    ('Example Queue 1', 'Tier_1'),
    ('Example Queue 2', 'Tier_2')
  AS t(queue_name, queue_tier)
),
queue_events AS (
  SELECT
    e.event_ts::date AS event_date,
    e.queue_name,
    EXTRACT(HOUR FROM e.event_start_ts) AS event_hour,
    e.talk_seconds,
    e.wait_seconds,
    e.abandon_seconds,
    e.hold_seconds
  FROM analytics.fact_queue_events e
  CROSS JOIN parameters p
  LEFT JOIN excluded_queues x ON e.queue_name = x.queue_name
  WHERE e.direction = p.direction_filter
    AND x.queue_name IS NULL
    AND e.event_ts::date >= p.start_date
)
SELECT
  q.event_date,
  q.queue_name,
  q.event_hour,
  COUNT(*) AS conversation_count,
  SUM(CASE WHEN q.talk_seconds > 0 THEN 1 ELSE 0 END) AS answered_count,
  SUM(CASE WHEN q.talk_seconds > 0 AND q.wait_seconds < sc.sla_seconds THEN 1 ELSE 0 END)
    AS answered_within_sla_count,
  SUM(CASE WHEN q.talk_seconds = 0 THEN 1 ELSE 0 END) AS abandoned_count,
  SUM(CASE WHEN q.abandon_seconds BETWEEN 1 AND p.quick_abandon_threshold_seconds THEN 1 ELSE 0 END)
    AS quick_abandon_count
FROM queue_events q
JOIN queue_dim d  ON q.queue_name = d.queue_name
JOIN sla_config sc ON d.queue_tier = sc.queue_tier
CROSS JOIN parameters p
GROUP BY 1,2,3
ORDER BY 1 DESC, 2, 3;
