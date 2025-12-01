-- Purpose:
--   Summarize outbound dialer activity, dials, connections, and agent time metrics
--   grouped by date and a randomized account bucket.
-- Notes:
--   - Original business outcomes consolidated into generic call result categories.

WITH dialer_load AS (
    SELECT
        CAST(load_timestamp AS DATE) AS load_date,
        SUM(record_count)            AS dialer_load_count
    FROM analytics.fact_dialer_load
    WHERE load_timestamp >= DATE '2024-08-01'
    GROUP BY CAST(load_timestamp AS DATE)
),

base_calls AS (
    SELECT
        CAST(c.call_end_timestamp AS DATE)          AS call_date,
        c.account_id                                 AS account_id,
        c.call_result                                AS call_result,
        c.agent_username                             AS agent_username,
        c.call_category                               AS call_category,
        c.call_sequence_number                        AS call_sequence_number,
        c.supervisor_id                               AS supervisor_id,
        c.call_end_timestamp                          AS call_end_time,
        c.agent_work_seconds                          AS agent_work_seconds,
        c.talk_seconds                                AS talk_seconds,
        c.wait_seconds                                AS wait_seconds,
        c.hold_seconds                                AS hold_seconds,
        c.wrap_seconds                                AS wrap_seconds,
        c.pause_seconds                               AS pause_seconds,
        c.linkback_seconds                            AS linkback_seconds,
        c.queue_wait_seconds                          AS queue_wait_seconds,
        c.agent_first_name || ' ' || c.agent_last_name AS agent_name,
        c.agent_team                                  AS agent_team,
        c.agent_location                              AS agent_location,
        c.call_flag                                   AS call_flag,
        a.random_bucket                               AS random_bucket
    FROM analytics.fact_outbound_calls c
    JOIN analytics.dim_account a
        ON c.account_id = a.account_id
    WHERE c.call_end_timestamp >= DATE '2024-08-01'
      AND a.is_deduped = 1
      AND a.is_daily_feed = 0
),

dials AS (
    SELECT
        b.call_date,
        b.random_bucket,
        COUNT(*)                        AS dial_count,
        COUNT(DISTINCT b.account_id)    AS unique_accounts_dialed,
        AVG(dl.dialer_load_count)       AS dialer_load_avg
    FROM base_calls b
    LEFT JOIN dialer_load dl
        ON dl.load_date = b.call_date
    WHERE 
        -- Generic dialer outcomes used for portfolio safety
        b.call_result ILIKE 'Answered%'
        OR b.call_result IN (
            'Busy',
            'Invalid',
            'No Answer',
            'System Error'
        )
        OR b.call_result ILIKE 'Machine%'
    GROUP BY 
        b.call_date,
        b.random_bucket
),

connects AS (
    SELECT
        b.call_date,
        b.random_bucket,
        COUNT(*)                                                AS connect_count,
        SUM(CASE WHEN b.call_category ILIKE '%Payment%' THEN 1 ELSE 0 END) AS payment_count,
        SUM(b.talk_seconds)                                     AS total_talk_seconds,
        SUM(b.hold_seconds)                                     AS total_hold_seconds,
        SUM(b.wrap_seconds)                                     AS total_wrap_seconds
    FROM base_calls b
    WHERE b.call_result = 'Answered - Connected'
    GROUP BY 
        b.call_date,
        b.random_bucket
)

SELECT
    d.call_date                            AS call_date,
    d.random_bucket                        AS random_bucket,
    d.dial_count                           AS dial_count,
    d.unique_accounts_dialed               AS unique_accounts_dialed,
    d.dialer_load_avg                      AS dialer_load_avg,
    COALESCE(c.connect_count, 0)           AS connect_count,
    COALESCE(c.payment_count, 0)           AS payment_count,
    c.total_talk_seconds,
    c.total_hold_seconds,
    c.total_wrap_seconds
FROM dials d
LEFT JOIN connects c
    ON c.call_date = d.call_date
   AND c.random_bucket = d.random_bucket
ORDER BY 
    d.call_date DESC,
    random_bucket ASC;
