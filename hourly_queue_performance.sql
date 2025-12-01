-- hourly_queue_performance.sql
-- Purpose:
--   Aggregate hourly queue performance metrics and transfer activity
--   using parameterized filters and excluded queues.

WITH parameters AS (
    SELECT 
        TO_DATE('2024-01-01') AS start_date,
        'inbound'             AS direction_filter
),

excluded_queues AS (
    SELECT COLUMN1 AS queue_name
    FROM VALUES
        ('Queue A'),
        ('Queue B'),
        ('Queue C')
),

transfers AS (
    SELECT
        fa.interaction_timestamp::date                                 AS interaction_date,
        fa.queue_name                                                  AS queue_name,
        EXTRACT(HOUR FROM TO_TIMESTAMP(fa.interaction_start_ts))       AS interaction_hour,
        SUM(fa.blind_transfer_count)                                   AS blind_transfer_count,
        SUM(fa.consult_transfer_count)                                 AS consult_transfer_count
    FROM analytics.fact_agent_interactions fa
    CROSS JOIN parameters p
    LEFT JOIN excluded_queues eq
        ON fa.queue_name = eq.queue_name
    WHERE
        fa.direction = p.direction_filter
        AND fa.agent_name IS NOT NULL
        AND eq.queue_name IS NULL
        AND fa.interaction_timestamp::date >= p.start_date
    GROUP BY 
        fa.interaction_timestamp::date,
        fa.queue_name,
        EXTRACT(HOUR FROM TO_TIMESTAMP(fa.interaction_start_ts))
),

acd_interactions AS (
    SELECT
        fi.interaction_timestamp::date                                 AS interaction_date,
        fi.queue_name                                                  AS queue_name,
        EXTRACT(HOUR FROM TO_TIMESTAMP(fi.interaction_start_ts))       AS interaction_hour,
        COUNT(*)                                                       AS row_count,
        COUNT(*)                                                       AS conversation_count,
        SUM(CASE WHEN fi.talk_seconds = 0 THEN 0 ELSE 1 END)          AS interactions_answered,
        SUM(
            CASE 
                WHEN fi.talk_seconds > 0 
                     AND (
                         (fi.queue_name = 'Primary Service Queue'      AND fi.wait_seconds < 90)
                         OR
                         (fi.queue_name <> 'Primary Service Queue'     AND fi.wait_seconds < 30)
                     )
                THEN 1 ELSE 0
            END
        )                                                              AS interactions_answered_within_sl,
        SUM(CASE WHEN fi.talk_seconds = 0 THEN 1 ELSE 0 END)          AS abandoned_interactions,
        SUM(CASE WHEN fi.abandon_seconds BETWEEN 1 AND 30 
                 THEN 1 ELSE 0 END)                                   AS quick_abandons,
        SUM(fi.talk_seconds)                                          AS total_talk_seconds,
        SUM(fi.hold_seconds)                                          AS total_hold_seconds,
        SUM(fi.wait_seconds)                                          AS total_wait_seconds,
        SUM(fi.abandon_seconds)                                       AS total_abandon_seconds,
        SUM(fi.flow_out_count)                                        AS flow_out_interactions
    FROM analytics.fact_acd_interactions fi
    CROSS JOIN parameters p
    LEFT JOIN excluded_queues eq
        ON fi.queue_name = eq.queue_name
    WHERE
        fi.direction = p.direction_filter
        AND eq.queue_name IS NULL
        AND fi.interaction_timestamp::date >= p.start_date
    GROUP BY 
        fi.interaction_timestamp::date,
        fi.queue_name,
        EXTRACT(HOUR FROM TO_TIMESTAMP(fi.interaction_start_ts))
)

SELECT
    ac.interaction_date,
    ac.queue_name,
    ac.interaction_hour,
    ac.row_count,
    ac.conversation_count,
    ac.interactions_answered,
    ac.interactions_answered_within_sl,
    ac.abandoned_interactions,
    ac.quick_abandons,
    ac.total_talk_seconds,
    ac.total_hold_seconds,
    ac.total_wait_seconds,
    ac.total_abandon_seconds,
    ac.flow_out_interactions,
    COALESCE(t.consult_transfer_count, 0) AS consult_transfer_count,
    COALESCE(t.blind_transfer_count, 0)   AS blind_transfer_count,
    'Snowflake'                           AS data_source
FROM acd_interactions ac
LEFT JOIN transfers t
    ON  ac.interaction_date = t.interaction_date
    AND ac.queue_name       = t.queue_name
    AND ac.interaction_hour = t.interaction_hour
ORDER BY 
    ac.interaction_date DESC,
    ac.queue_name       ASC,
    ac.interaction_hour ASC;
