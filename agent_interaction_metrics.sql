-------------------------------
-- 1. Daily agent metrics
-------------------------------

-- Purpose:
--   1) Daily entity-level interaction metrics with time-in-state breakdown.

SELECT
    event_ts::date                                AS event_date,
    entity_id                                     AS entity_id,
    org_unit                                      AS org_unit,
    MIN(group_name)                               AS group_name,
    COUNT(*)                                      AS conversation_count,
    SUM(metric_handle_seconds)                    AS handle_seconds,
    SUM(metric_active_seconds)                    AS active_seconds,
    SUM(metric_hold_seconds)                      AS hold_seconds,
    SUM(metric_wrapup_seconds)                    AS wrapup_seconds,
    SUM(metric_action_type1_count)                AS action_type1_count,
    SUM(metric_action_type2_count)                AS action_type2_count,
    CASE
        WHEN raw_location IN ('LocA_raw', 'Location A (raw)') THEN 'Location A'
        WHEN raw_location IN ('LocB_raw', 'Location B (raw)') THEN 'Location B'
        ELSE raw_location
    END                                           AS location_clean,
    'example'                                     AS data_source
FROM analytics.fact_events
WHERE event_ts::date >= DATE '2024-01-01'
GROUP BY
    event_ts::date,
    entity_id,
    org_unit,
    CASE
        WHEN raw_location IN ('LocA_raw', 'Location A (raw)') THEN 'Location A'
        WHEN raw_location IN ('LocB_raw', 'Location B (raw)') THEN 'Location B'
        ELSE raw_location
    END
ORDER BY
    event_date ASC,
    entity_id ASC;


-------------------------------
-- 2. Monthly call volume by queue
-------------------------------

-- Purpose:
--   Monthly event volume by category

SELECT
    DATE_TRUNC('month', event_ts)                 AS month_start,
    category_name                                 AS category_name,
    COUNT(*)                                      AS event_count
FROM analytics.fact_events
WHERE DATE_TRUNC('month', event_ts) = DATE '2024-08-01'
GROUP BY
    month_start,
    category_name
ORDER BY
    event_count DESC;
