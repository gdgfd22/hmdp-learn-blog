SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '5min';
SET 'execution.checkpointing.min-pause' = '3s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'table.exec.state.ttl' = '2d';
SET 'pipeline.name' = 'hmdp-dwd-clean-and-cdc';

CREATE TABLE ods_behavior_event (
    event_id STRING,
    event_type STRING,
    user_id BIGINT,
    device_id STRING,
    shop_id BIGINT,
    blog_id BIGINT,
    voucher_id BIGINT,
    order_id BIGINT,
    `result` STRING,
    event_time BIGINT,
    ingest_time BIGINT,
    properties MAP<STRING, STRING>,
    event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    ingest_ts AS TO_TIMESTAMP_LTZ(ingest_time, 3),
    process_ts AS PROCTIME(),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_behavior_event',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'hmdp-dwd-behavior',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.fail-on-missing-field' = 'false'
);

CREATE TABLE mysql_order_cdc (
    id BIGINT,
    user_id BIGINT,
    voucher_id BIGINT,
    shop_id BIGINT,
    original_amount BIGINT,
    pay_amount BIGINT,
    discount_amount BIGINT,
    refund_amount BIGINT,
    pay_type INT,
    status INT,
    create_time TIMESTAMP(3),
    pay_time TIMESTAMP(3),
    use_time TIMESTAMP(3),
    refund_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    process_ts AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'flink',
    'password' = 'flink_pw',
    'database-name' = 'hmdp',
    'table-name' = 'tb_voucher_order',
    'server-time-zone' = 'Asia/Shanghai',
    'server-id' = '5401-5404',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE mysql_voucher_cdc (
    id BIGINT,
    shop_id BIGINT,
    pay_value BIGINT,
    actual_value BIGINT,
    `type` INT,
    status INT,
    update_time TIMESTAMP(3),
    process_ts AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'flink',
    'password' = 'flink_pw',
    'database-name' = 'hmdp',
    'table-name' = 'tb_voucher',
    'server-time-zone' = 'Asia/Shanghai',
    'server-id' = '5411-5414',
    'scan.startup.mode' = 'initial'
);

CREATE TEMPORARY VIEW normalized_behavior AS
SELECT
    event_id,
    event_type,
    user_id,
    CASE
        WHEN user_id IS NOT NULL THEN CONCAT('u:', CAST(user_id AS STRING))
        WHEN device_id IS NOT NULL AND CHAR_LENGTH(TRIM(device_id)) > 0 THEN CONCAT('d:', TRIM(device_id))
        ELSE NULL
    END AS user_key,
    device_id,
    shop_id,
    blog_id,
    voucher_id,
    order_id,
    `result`,
    properties['reason'] AS reason,
    event_time,
    ingest_time,
    event_ts,
    ingest_ts,
    process_ts,
    CASE
        WHEN event_id IS NULL OR CHAR_LENGTH(TRIM(event_id)) = 0 THEN 'EMPTY_EVENT_ID'
        WHEN event_type NOT IN ('SHOP_VIEW', 'BLOG_VIEW', 'BLOG_LIKE', 'BLOG_UNLIKE',
                                'FOLLOW', 'UNFOLLOW', 'VOUCHER_EXPOSURE', 'SECKILL_REQUEST') THEN 'ILLEGAL_EVENT_TYPE'
        WHEN user_id IS NULL AND (device_id IS NULL OR CHAR_LENGTH(TRIM(device_id)) = 0) THEN 'MISSING_USER_KEY'
        WHEN event_time IS NULL OR ingest_time IS NULL THEN 'MISSING_EVENT_TIME'
        WHEN event_ts > CURRENT_TIMESTAMP + INTERVAL '5' MINUTE THEN 'FUTURE_EVENT_TIME'
        WHEN event_ts < CURRENT_TIMESTAMP - INTERVAL '7' DAY THEN 'EXPIRED_EVENT_TIME'
        WHEN event_type = 'SHOP_VIEW' AND shop_id IS NULL THEN 'MISSING_SHOP_ID'
        WHEN event_type IN ('BLOG_VIEW', 'BLOG_LIKE', 'BLOG_UNLIKE') AND blog_id IS NULL THEN 'MISSING_BLOG_ID'
        WHEN event_type IN ('VOUCHER_EXPOSURE', 'SECKILL_REQUEST') AND voucher_id IS NULL THEN 'MISSING_VOUCHER_ID'
        ELSE NULL
    END AS error_reason
FROM ods_behavior_event;

CREATE TEMPORARY VIEW valid_behavior AS
SELECT event_id, event_type, user_id, user_key, device_id, shop_id, blog_id, voucher_id,
       order_id, `result`, reason, event_time, ingest_time, event_ts, ingest_ts, process_ts
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts ASC) AS row_num
    FROM normalized_behavior
    WHERE error_reason IS NULL
)
WHERE row_num = 1;

CREATE TEMPORARY VIEW invalid_behavior AS
SELECT * FROM normalized_behavior WHERE error_reason IS NOT NULL;

CREATE TEMPORARY VIEW normalized_order AS
SELECT *,
       CASE
           WHEN user_id IS NULL OR voucher_id IS NULL OR shop_id IS NULL THEN 'ORDER_KEY_NULL'
           WHEN status NOT BETWEEN 1 AND 6 THEN 'ILLEGAL_ORDER_STATUS'
           WHEN original_amount < 0 OR pay_amount < 0 OR discount_amount < 0 OR refund_amount < 0 THEN 'NEGATIVE_ORDER_AMOUNT'
           WHEN pay_amount + discount_amount > original_amount THEN 'ORDER_AMOUNT_MISMATCH'
           WHEN refund_amount > pay_amount THEN 'REFUND_EXCEEDS_PAYMENT'
           WHEN status IN (2, 3, 5, 6) AND pay_time IS NULL THEN 'PAID_STATUS_WITHOUT_PAY_TIME'
           WHEN status = 6 AND refund_time IS NULL THEN 'REFUND_STATUS_WITHOUT_REFUND_TIME'
           ELSE NULL
       END AS error_reason
FROM mysql_order_cdc;

CREATE TEMPORARY VIEW invalid_order AS
SELECT * FROM normalized_order
WHERE user_id IS NULL OR voucher_id IS NULL OR shop_id IS NULL
   OR status NOT BETWEEN 1 AND 6
   OR original_amount < 0 OR pay_amount < 0 OR discount_amount < 0 OR refund_amount < 0
   OR pay_amount + discount_amount > original_amount
   OR refund_amount > pay_amount
   OR (status IN (2, 3, 5, 6) AND pay_time IS NULL)
   OR (status = 6 AND refund_time IS NULL);

CREATE TEMPORARY VIEW valid_order AS
SELECT id, user_id, voucher_id, shop_id, original_amount, pay_amount, discount_amount,
       refund_amount, pay_type, status, create_time, pay_time, use_time, refund_time, update_time
FROM normalized_order
WHERE error_reason IS NULL;

CREATE TEMPORARY VIEW invalid_voucher AS
SELECT *,
       CASE
           WHEN shop_id IS NULL THEN 'VOUCHER_SHOP_NULL'
           WHEN `type` NOT IN (0, 1) THEN 'ILLEGAL_VOUCHER_TYPE'
           WHEN status NOT BETWEEN 1 AND 3 THEN 'ILLEGAL_VOUCHER_STATUS'
           WHEN pay_value < 0 OR actual_value < 0 OR pay_value > actual_value THEN 'VOUCHER_AMOUNT_MISMATCH'
           ELSE NULL
       END AS error_reason
FROM mysql_voucher_cdc
WHERE shop_id IS NULL OR `type` NOT IN (0, 1) OR status NOT BETWEEN 1 AND 3
   OR pay_value < 0 OR actual_value < 0 OR pay_value > actual_value;

CREATE TABLE dwd_behavior_kafka (
    event_id STRING,
    event_type STRING,
    user_id BIGINT,
    user_key STRING,
    device_id STRING,
    shop_id BIGINT,
    blog_id BIGINT,
    voucher_id BIGINT,
    order_id BIGINT,
    `result` STRING,
    reason STRING,
    event_time BIGINT,
    ingest_time BIGINT,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_behavior_event',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

CREATE TABLE dwd_order_kafka (
    id BIGINT,
    user_id BIGINT,
    voucher_id BIGINT,
    shop_id BIGINT,
    original_amount BIGINT,
    pay_amount BIGINT,
    discount_amount BIGINT,
    refund_amount BIGINT,
    pay_type INT,
    status INT,
    create_time TIMESTAMP(3),
    pay_time TIMESTAMP(3),
    use_time TIMESTAMP(3),
    refund_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_order_change',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

CREATE TABLE dirty_behavior_kafka (
    event_id STRING,
    event_type STRING,
    user_id BIGINT,
    device_id STRING,
    event_time BIGINT,
    ingest_time BIGINT,
    error_reason STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'dirty_behavior_event',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

CREATE TABLE dwd_behavior_doris (
    event_id STRING,
    event_type STRING,
    user_id BIGINT,
    user_key STRING,
    device_id STRING,
    shop_id BIGINT,
    blog_id BIGINT,
    voucher_id BIGINT,
    order_id BIGINT,
    `result` STRING,
    reason STRING,
    event_time TIMESTAMP(3),
    ingest_time TIMESTAMP(3),
    process_time TIMESTAMP(3),
    latency_ms BIGINT,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dwd_user_behavior_detail',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'hmdp_dwd_behavior',
    'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dwd_order_doris (
    id BIGINT,
    user_id BIGINT,
    voucher_id BIGINT,
    shop_id BIGINT,
    original_amount BIGINT,
    pay_amount BIGINT,
    discount_amount BIGINT,
    refund_amount BIGINT,
    pay_type INT,
    status INT,
    create_time TIMESTAMP(3),
    pay_time TIMESTAMP(3),
    use_time TIMESTAMP(3),
    refund_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dwd_order_detail',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'hmdp_dwd_order',
    'sink.enable-2pc' = 'true',
    'sink.enable-delete' = 'true',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true'
);

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO dwd_behavior_kafka
    SELECT event_id, event_type, user_id, user_key, device_id, shop_id, blog_id,
           voucher_id, order_id, `result`, reason, event_time, ingest_time
    FROM valid_behavior;

    INSERT INTO dwd_behavior_doris
    SELECT event_id, event_type, user_id, user_key, device_id, shop_id, blog_id,
           voucher_id, order_id, `result`, reason,
           CAST(event_ts AS TIMESTAMP(3)), CAST(ingest_ts AS TIMESTAMP(3)),
           CAST(process_ts AS TIMESTAMP(3)), ingest_time - event_time
    FROM valid_behavior;

    INSERT INTO dirty_behavior_kafka
    SELECT event_id, event_type, user_id, device_id, event_time, ingest_time, error_reason
    FROM invalid_behavior;

    INSERT INTO dwd_order_kafka
    SELECT id, user_id, voucher_id, shop_id, original_amount, pay_amount, discount_amount,
           refund_amount, pay_type, status, create_time, pay_time, use_time, refund_time, update_time
    FROM valid_order;

    INSERT INTO dwd_order_doris
    SELECT id, user_id, voucher_id, shop_id, original_amount, pay_amount, discount_amount,
           refund_amount, pay_type, status, create_time, pay_time, use_time, refund_time, update_time
    FROM valid_order;
END;
