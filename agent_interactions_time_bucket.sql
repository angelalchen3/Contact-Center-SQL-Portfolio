-- Purpose:
--   Aggregate operational events by 30-minute interval, org unit, and location.
--   (Clean-room example: generic schema/fields, no company-specific identifiers.)

SELECT
    event_ts::date                               AS event_date,
    time_bucket_30min                            AS time_interval_30min,
    org_unit                                     AS org_unit,
    CASE
        WHEN raw_location IN ('Location A - raw', 'LocA') THEN 'Location A'
        WHEN raw_location IN ('Location B - raw', 'LocB') THEN 'Location B'
        ELSE raw_location
    END                                           AS location_clean,
    COUNT(*)                                      AS event_count,
    SUM(metric_handle_seconds)                    AS total_handle_seconds,
    SUM(metric_talk_seconds)                      AS total_talk_seconds,
    SUM(metric_hold_seconds)                      AS total_hold_seconds,
    SUM(metric_wrapup_seconds)                    AS total_wrapup_seconds,
    SUM(metric_transfer_type1_count)              AS transfer_type1_count,
    SUM(metric_transfer_type2_count)              AS transfer_type2_count,
    'warehouse'                                   AS data_source
FROM analytics.fact_events
WHERE event_direction = 'inbound'
  AND entity_name IS NOT NULL
  -- Example exclusion pattern (generic placeholders)
  AND entity_name NOT IN ('Example Entity 1', 'Example Entity 2', 'Example Entity 3')
  -- Example category exclusion pattern (generic placeholders)
  AND category_name NOT IN ('Example Category A', 'Example Category B', 'Example Category C')
  AND event_ts::date >= DATE '2024-01-01'
  AND org_unit = 'Example Org Unit'
GROUP BY
    event_ts::date,
    time_bucket_30min,
    org_unit,
    location_clean
ORDER BY
    event_date DESC,
    time_interval_30min ASC;
