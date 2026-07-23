SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '5min';
SET 'execution.checkpointing.min-pause' = '3s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'table.exec.state.ttl' = '8d';
SET 'pipeline.name' = 'hmdp-dws-realtime-aggregate';

CREATE TABLE dwd_behavior (
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
    event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_behavior_event',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'hmdp-dws-behavior',
    'properties.auto.offset.reset' = 'earliest',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

CREATE TABLE dwd_order (
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
    'properties.group.id' = 'hmdp-dws-order',
    'properties.auto.offset.reset' = 'earliest',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'ALL'
);

CREATE TABLE dws_platform_day_sink (
    metric_date DATE,
    dau BIGINT,
    shop_visit_pv BIGINT,
    voucher_exposure_count BIGINT,
    seckill_request_count BIGINT,
    seckill_accepted_count BIGINT,
    like_count BIGINT,
    unlike_count BIGINT,
    follow_count BIGINT,
    unfollow_count BIGINT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (metric_date) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dws_platform_day', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_dws_platform', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_user_active_day_sink (
    metric_date DATE,
    user_key STRING,
    first_event_time TIMESTAMP(3),
    last_event_time TIMESTAMP(3),
    event_count BIGINT,
    PRIMARY KEY (metric_date, user_key) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dws_user_active_day', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_dws_active', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_shop_day_sink (
    metric_date DATE,
    shop_id BIGINT,
    visit_pv BIGINT,
    visit_uv BIGINT,
    blog_view_count BIGINT,
    like_count BIGINT,
    unlike_count BIGINT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (metric_date, shop_id) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dws_shop_day', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_dws_shop', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_blog_day_sink (
    metric_date DATE,
    blog_id BIGINT,
    shop_id BIGINT,
    view_count BIGINT,
    view_uv BIGINT,
    like_count BIGINT,
    unlike_count BIGINT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (metric_date, blog_id) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dws_blog_day', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_dws_blog', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_voucher_day_sink (
    metric_date DATE,
    voucher_id BIGINT,
    shop_id BIGINT,
    exposure_count BIGINT,
    seckill_request_count BIGINT,
    accepted_count BIGINT,
    rejected_count BIGINT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (metric_date, voucher_id) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dws_voucher_behavior_day', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_dws_voucher', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

CREATE TABLE dws_order_day_sink (
    metric_date DATE,
    voucher_id BIGINT,
    shop_id BIGINT,
    order_count BIGINT,
    paid_order_count BIGINT,
    gmv BIGINT,
    refund_amount BIGINT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (metric_date, voucher_id, shop_id) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.dws_order_day', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_dws_order', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO dws_platform_day_sink
    SELECT
        CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE),
        COUNT(DISTINCT user_key),
        SUM(CASE WHEN event_type = 'SHOP_VIEW' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event_type = 'VOUCHER_EXPOSURE' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event_type = 'SECKILL_REQUEST' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event_type = 'SECKILL_REQUEST' AND `result` = 'ACCEPTED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event_type = 'BLOG_LIKE' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event_type = 'BLOG_UNLIKE' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event_type = 'FOLLOW' THEN 1 ELSE 0 END),
        SUM(CASE WHEN event_type = 'UNFOLLOW' THEN 1 ELSE 0 END),
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
    FROM dwd_behavior
    GROUP BY CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE);

    INSERT INTO dws_user_active_day_sink
    SELECT CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), user_key,
           CAST(MIN(event_ts) AS TIMESTAMP(3)), CAST(MAX(event_ts) AS TIMESTAMP(3)), COUNT(*)
    FROM dwd_behavior
    GROUP BY CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), user_key;

    INSERT INTO dws_shop_day_sink
    SELECT CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), shop_id,
           SUM(CASE WHEN event_type = 'SHOP_VIEW' THEN 1 ELSE 0 END),
           COUNT(DISTINCT CASE WHEN event_type = 'SHOP_VIEW' THEN user_key ELSE NULL END),
           SUM(CASE WHEN event_type = 'BLOG_VIEW' THEN 1 ELSE 0 END),
           SUM(CASE WHEN event_type = 'BLOG_LIKE' THEN 1 ELSE 0 END),
           SUM(CASE WHEN event_type = 'BLOG_UNLIKE' THEN 1 ELSE 0 END),
           CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
    FROM dwd_behavior
    WHERE shop_id IS NOT NULL
    GROUP BY CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), shop_id;

    INSERT INTO dws_blog_day_sink
    SELECT CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), blog_id, MAX(shop_id),
           SUM(CASE WHEN event_type = 'BLOG_VIEW' THEN 1 ELSE 0 END),
           COUNT(DISTINCT CASE WHEN event_type = 'BLOG_VIEW' THEN user_key ELSE NULL END),
           SUM(CASE WHEN event_type = 'BLOG_LIKE' THEN 1 ELSE 0 END),
           SUM(CASE WHEN event_type = 'BLOG_UNLIKE' THEN 1 ELSE 0 END),
           CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
    FROM dwd_behavior
    WHERE blog_id IS NOT NULL
    GROUP BY CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), blog_id;

    INSERT INTO dws_voucher_day_sink
    SELECT CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), voucher_id, MAX(shop_id),
           SUM(CASE WHEN event_type = 'VOUCHER_EXPOSURE' THEN 1 ELSE 0 END),
           SUM(CASE WHEN event_type = 'SECKILL_REQUEST' THEN 1 ELSE 0 END),
           SUM(CASE WHEN event_type = 'SECKILL_REQUEST' AND `result` = 'ACCEPTED' THEN 1 ELSE 0 END),
           SUM(CASE WHEN event_type = 'SECKILL_REQUEST' AND `result` = 'REJECTED' THEN 1 ELSE 0 END),
           CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
    FROM dwd_behavior
    WHERE voucher_id IS NOT NULL
    GROUP BY CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE), voucher_id;

    INSERT INTO dws_order_day_sink
    SELECT CAST(DATE_FORMAT(create_time, 'yyyy-MM-dd') AS DATE), voucher_id, shop_id,
           COUNT(*),
           SUM(CASE WHEN pay_time IS NOT NULL THEN 1 ELSE 0 END),
           SUM(CASE WHEN pay_time IS NOT NULL THEN pay_amount ELSE 0 END),
           SUM(refund_amount),
           CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
    FROM dwd_order
    GROUP BY CAST(DATE_FORMAT(create_time, 'yyyy-MM-dd') AS DATE), voucher_id, shop_id;
END;
