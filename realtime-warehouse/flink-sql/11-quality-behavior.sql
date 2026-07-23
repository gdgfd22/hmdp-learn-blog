SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'hmdp-quality-invalid-behavior';

CREATE TABLE ods_behavior_event (
    event_id STRING, event_type STRING, user_id BIGINT, device_id STRING,
    shop_id BIGINT, blog_id BIGINT, voucher_id BIGINT, order_id BIGINT,
    `result` STRING, event_time BIGINT, ingest_time BIGINT,
    properties MAP<STRING, STRING>,
    event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    quality_time AS CAST(DATE_FORMAT(TO_TIMESTAMP_LTZ(ingest_time, 3), 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3))
) WITH (
    'connector' = 'kafka', 'topic' = 'ods_behavior_event',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'hmdp-quality-invalid-behavior',
    'scan.startup.mode' = 'earliest-offset', 'format' = 'json',
    'json.ignore-parse-errors' = 'true', 'json.fail-on-missing-field' = 'false'
);

CREATE TEMPORARY VIEW invalid_behavior AS
SELECT *,
       CASE
           WHEN event_id IS NULL OR CHAR_LENGTH(TRIM(event_id)) = 0 THEN 'EMPTY_EVENT_ID'
           WHEN event_type NOT IN ('SHOP_VIEW', 'BLOG_VIEW', 'BLOG_LIKE', 'BLOG_UNLIKE', 'FOLLOW', 'UNFOLLOW', 'VOUCHER_EXPOSURE', 'SECKILL_REQUEST') THEN 'ILLEGAL_EVENT_TYPE'
           WHEN user_id IS NULL AND (device_id IS NULL OR CHAR_LENGTH(TRIM(device_id)) = 0) THEN 'MISSING_USER_KEY'
           WHEN event_time IS NULL OR ingest_time IS NULL THEN 'MISSING_EVENT_TIME'
           WHEN event_ts > CURRENT_TIMESTAMP + INTERVAL '5' MINUTE THEN 'FUTURE_EVENT_TIME'
           WHEN event_ts < CURRENT_TIMESTAMP - INTERVAL '7' DAY THEN 'EXPIRED_EVENT_TIME'
           WHEN event_type = 'SHOP_VIEW' AND shop_id IS NULL THEN 'MISSING_SHOP_ID'
           WHEN event_type IN ('BLOG_VIEW', 'BLOG_LIKE', 'BLOG_UNLIKE') AND blog_id IS NULL THEN 'MISSING_BLOG_ID'
           WHEN event_type IN ('VOUCHER_EXPOSURE', 'SECKILL_REQUEST') AND voucher_id IS NULL THEN 'MISSING_VOUCHER_ID'
           ELSE NULL
       END AS error_reason
FROM ods_behavior_event
WHERE event_id IS NULL OR CHAR_LENGTH(TRIM(event_id)) = 0
   OR event_type NOT IN ('SHOP_VIEW', 'BLOG_VIEW', 'BLOG_LIKE', 'BLOG_UNLIKE', 'FOLLOW', 'UNFOLLOW', 'VOUCHER_EXPOSURE', 'SECKILL_REQUEST')
   OR (user_id IS NULL AND (device_id IS NULL OR CHAR_LENGTH(TRIM(device_id)) = 0))
   OR event_time IS NULL OR ingest_time IS NULL
   OR event_ts > CURRENT_TIMESTAMP + INTERVAL '5' MINUTE OR event_ts < CURRENT_TIMESTAMP - INTERVAL '7' DAY
   OR (event_type = 'SHOP_VIEW' AND shop_id IS NULL)
   OR (event_type IN ('BLOG_VIEW', 'BLOG_LIKE', 'BLOG_UNLIKE') AND blog_id IS NULL)
   OR (event_type IN ('VOUCHER_EXPOSURE', 'SECKILL_REQUEST') AND voucher_id IS NULL);

CREATE TABLE quality_doris (
    check_time TIMESTAMP(3), check_name STRING, error_count BIGINT, sample_message STRING,
    PRIMARY KEY (check_time, check_name) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.ads_data_quality', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_quality_behavior', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

INSERT INTO quality_doris
SELECT COALESCE(quality_time, CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3))),
       'INVALID_BEHAVIOR_EVENT', COUNT(*),
       MAX(CONCAT(error_reason, ':', COALESCE(event_id, 'NULL')))
FROM invalid_behavior
GROUP BY COALESCE(quality_time, CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3)));
