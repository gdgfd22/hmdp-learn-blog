CREATE DATABASE IF NOT EXISTS hmdp_analytics;
USE hmdp_analytics;

CREATE TABLE IF NOT EXISTS dwd_user_behavior_detail (
    event_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    user_id BIGINT NULL,
    user_key VARCHAR(80) NOT NULL,
    device_id VARCHAR(64) NULL,
    shop_id BIGINT NULL,
    blog_id BIGINT NULL,
    voucher_id BIGINT NULL,
    order_id BIGINT NULL,
    result VARCHAR(32) NULL,
    reason VARCHAR(64) NULL,
    event_time DATETIME(3) NOT NULL,
    ingest_time DATETIME(3) NOT NULL,
    process_time DATETIME(3) NOT NULL,
    latency_ms BIGINT NOT NULL
)
UNIQUE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS dwd_order_detail (
    id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    voucher_id BIGINT NOT NULL,
    shop_id BIGINT NOT NULL,
    original_amount BIGINT NOT NULL,
    pay_amount BIGINT NOT NULL,
    discount_amount BIGINT NOT NULL,
    refund_amount BIGINT NOT NULL,
    pay_type INT NOT NULL,
    status INT NOT NULL,
    create_time DATETIME NOT NULL,
    pay_time DATETIME NULL,
    use_time DATETIME NULL,
    refund_time DATETIME NULL,
    update_time DATETIME NOT NULL
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS dws_platform_day (
    metric_date DATE NOT NULL,
    dau BIGINT NOT NULL,
    shop_visit_pv BIGINT NOT NULL,
    voucher_exposure_count BIGINT NOT NULL,
    seckill_request_count BIGINT NOT NULL,
    seckill_accepted_count BIGINT NOT NULL,
    like_count BIGINT NOT NULL,
    unlike_count BIGINT NOT NULL,
    follow_count BIGINT NOT NULL,
    unfollow_count BIGINT NOT NULL,
    update_time DATETIME NOT NULL
)
UNIQUE KEY(metric_date)
DISTRIBUTED BY HASH(metric_date) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS dws_user_active_day (
    metric_date DATE NOT NULL,
    user_key VARCHAR(80) NOT NULL,
    first_event_time DATETIME(3) NOT NULL,
    last_event_time DATETIME(3) NOT NULL,
    event_count BIGINT NOT NULL
)
UNIQUE KEY(metric_date, user_key)
DISTRIBUTED BY HASH(user_key) BUCKETS 3
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS dws_shop_day (
    metric_date DATE NOT NULL,
    shop_id BIGINT NOT NULL,
    visit_pv BIGINT NOT NULL,
    visit_uv BIGINT NOT NULL,
    blog_view_count BIGINT NOT NULL,
    like_count BIGINT NOT NULL,
    unlike_count BIGINT NOT NULL,
    update_time DATETIME NOT NULL
)
UNIQUE KEY(metric_date, shop_id)
DISTRIBUTED BY HASH(shop_id) BUCKETS 3
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS dws_blog_day (
    metric_date DATE NOT NULL,
    blog_id BIGINT NOT NULL,
    shop_id BIGINT NULL,
    view_count BIGINT NOT NULL,
    view_uv BIGINT NOT NULL,
    like_count BIGINT NOT NULL,
    unlike_count BIGINT NOT NULL,
    update_time DATETIME NOT NULL
)
UNIQUE KEY(metric_date, blog_id)
DISTRIBUTED BY HASH(blog_id) BUCKETS 3
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS dws_voucher_behavior_day (
    metric_date DATE NOT NULL,
    voucher_id BIGINT NOT NULL,
    shop_id BIGINT NULL,
    exposure_count BIGINT NOT NULL,
    seckill_request_count BIGINT NOT NULL,
    accepted_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL,
    update_time DATETIME NOT NULL
)
UNIQUE KEY(metric_date, voucher_id)
DISTRIBUTED BY HASH(voucher_id) BUCKETS 3
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS dws_order_day (
    metric_date DATE NOT NULL,
    voucher_id BIGINT NOT NULL,
    shop_id BIGINT NOT NULL,
    order_count BIGINT NOT NULL,
    paid_order_count BIGINT NOT NULL,
    gmv BIGINT NOT NULL,
    refund_amount BIGINT NOT NULL,
    update_time DATETIME NOT NULL
)
UNIQUE KEY(metric_date, voucher_id, shop_id)
DISTRIBUTED BY HASH(voucher_id) BUCKETS 3
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

CREATE TABLE IF NOT EXISTS ads_data_quality (
    check_time DATETIME(3) NOT NULL,
    check_name VARCHAR(64) NOT NULL,
    error_count BIGINT NOT NULL,
    sample_message VARCHAR(512) NULL
)
UNIQUE KEY(check_time, check_name)
DISTRIBUTED BY HASH(check_name) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");

DROP VIEW IF EXISTS ads_realtime_overview;
CREATE VIEW ads_realtime_overview AS
SELECT
    p.metric_date,
    p.dau,
    p.shop_visit_pv,
    p.voucher_exposure_count,
    p.seckill_request_count,
    p.seckill_accepted_count,
    p.like_count,
    p.unlike_count,
    p.follow_count,
    p.unfollow_count,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.paid_order_count, 0) AS paid_order_count,
    COALESCE(o.gmv, 0) AS gmv,
    COALESCE(o.refund_amount, 0) AS refund_amount,
    CASE WHEN p.seckill_request_count = 0 THEN 0
         ELSE ROUND(COALESCE(o.order_count, 0) * 100.0 / p.seckill_request_count, 2) END AS seckill_success_rate
FROM dws_platform_day p
LEFT JOIN (
    SELECT metric_date,
           SUM(order_count) AS order_count,
           SUM(paid_order_count) AS paid_order_count,
           SUM(gmv) AS gmv,
           SUM(refund_amount) AS refund_amount
    FROM dws_order_day
    GROUP BY metric_date
) o ON p.metric_date = o.metric_date;

DROP VIEW IF EXISTS ads_shop_rank;
CREATE VIEW ads_shop_rank AS
SELECT
    s.metric_date,
    s.shop_id,
    s.visit_pv,
    s.visit_uv,
    s.like_count - s.unlike_count AS net_like_count,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.gmv, 0) AS gmv,
    s.visit_uv + (s.like_count - s.unlike_count) * 3 + COALESCE(o.order_count, 0) * 5 AS hot_score
FROM dws_shop_day s
LEFT JOIN (
    SELECT metric_date, shop_id, SUM(order_count) AS order_count, SUM(gmv) AS gmv
    FROM dws_order_day
    GROUP BY metric_date, shop_id
) o ON s.metric_date = o.metric_date AND s.shop_id = o.shop_id;

DROP VIEW IF EXISTS ads_blog_rank;
CREATE VIEW ads_blog_rank AS
SELECT
    metric_date,
    blog_id,
    shop_id,
    view_count,
    view_uv,
    like_count - unlike_count AS net_like_count,
    view_uv + (like_count - unlike_count) * 3 AS hot_score
FROM dws_blog_day;

DROP VIEW IF EXISTS ads_voucher_funnel;
CREATE VIEW ads_voucher_funnel AS
SELECT
    b.metric_date,
    b.voucher_id,
    b.shop_id,
    b.exposure_count,
    b.seckill_request_count,
    b.accepted_count,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.paid_order_count, 0) AS paid_order_count,
    COALESCE(o.gmv, 0) AS gmv,
    CASE WHEN b.exposure_count = 0 THEN 0 ELSE ROUND(b.seckill_request_count * 100.0 / b.exposure_count, 2) END AS request_rate,
    CASE WHEN b.seckill_request_count = 0 THEN 0 ELSE ROUND(COALESCE(o.order_count, 0) * 100.0 / b.seckill_request_count, 2) END AS order_rate,
    CASE WHEN COALESCE(o.order_count, 0) = 0 THEN 0 ELSE ROUND(COALESCE(o.paid_order_count, 0) * 100.0 / o.order_count, 2) END AS pay_rate
FROM dws_voucher_behavior_day b
LEFT JOIN dws_order_day o
  ON b.metric_date = o.metric_date AND b.voucher_id = o.voucher_id AND b.shop_id = o.shop_id;

DROP VIEW IF EXISTS ads_user_retention;
CREATE VIEW ads_user_retention AS
SELECT
    cohort.first_active_date AS cohort_date,
    COUNT(*) AS cohort_size,
    SUM(CASE WHEN d1.user_key IS NULL THEN 0 ELSE 1 END) AS d1_retained_users,
    SUM(CASE WHEN d7.user_key IS NULL THEN 0 ELSE 1 END) AS d7_retained_users,
    ROUND(SUM(CASE WHEN d1.user_key IS NULL THEN 0 ELSE 1 END) * 100.0 / COUNT(*), 2) AS d1_retention_rate,
    ROUND(SUM(CASE WHEN d7.user_key IS NULL THEN 0 ELSE 1 END) * 100.0 / COUNT(*), 2) AS d7_retention_rate
FROM (
    SELECT user_key, MIN(metric_date) AS first_active_date
    FROM dws_user_active_day
    GROUP BY user_key
) cohort
LEFT JOIN dws_user_active_day d1
  ON cohort.user_key = d1.user_key AND d1.metric_date = DATE_ADD(cohort.first_active_date, INTERVAL 1 DAY)
LEFT JOIN dws_user_active_day d7
  ON cohort.user_key = d7.user_key AND d7.metric_date = DATE_ADD(cohort.first_active_date, INTERVAL 7 DAY)
GROUP BY cohort.first_active_date;
