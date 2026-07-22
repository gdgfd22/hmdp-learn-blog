SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'hmdp-quality-duplicate-event';

CREATE TABLE ods_behavior_event (
    event_id STRING, ingest_time BIGINT,
    quality_time AS CAST(DATE_FORMAT(TO_TIMESTAMP_LTZ(ingest_time, 3), 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3))
) WITH (
    'connector' = 'kafka', 'topic' = 'ods_behavior_event',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'hmdp-quality-duplicate-event',
    'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TABLE quality_doris (
    check_time TIMESTAMP(3), check_name STRING, error_count BIGINT, sample_message STRING,
    PRIMARY KEY (check_time, check_name) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.ads_data_quality', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_quality_duplicate', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

INSERT INTO quality_doris
SELECT quality_time, 'DUPLICATE_EVENT_ID', SUM(duplicate_count - 1), MAX(event_id)
FROM (
    SELECT quality_time, event_id, COUNT(*) AS duplicate_count
    FROM ods_behavior_event
    WHERE event_id IS NOT NULL
    GROUP BY quality_time, event_id
    HAVING COUNT(*) > 1
)
GROUP BY quality_time;
