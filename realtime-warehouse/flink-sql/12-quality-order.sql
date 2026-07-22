SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'hmdp-quality-invalid-order';

CREATE TABLE mysql_order_cdc (
    id BIGINT, user_id BIGINT, voucher_id BIGINT, shop_id BIGINT,
    original_amount BIGINT, pay_amount BIGINT, discount_amount BIGINT, refund_amount BIGINT,
    status INT, pay_time TIMESTAMP(3), refund_time TIMESTAMP(3), update_time TIMESTAMP(3),
    quality_time AS CAST(DATE_FORMAT(update_time, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3)),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc', 'hostname' = 'mysql', 'port' = '3306',
    'username' = 'flink', 'password' = 'flink_pw', 'database-name' = 'hmdp',
    'table-name' = 'tb_voucher_order', 'server-time-zone' = 'Asia/Shanghai',
    'server-id' = '5421-5424', 'scan.startup.mode' = 'initial'
);

CREATE TEMPORARY VIEW invalid_order AS
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
FROM mysql_order_cdc
WHERE user_id IS NULL OR voucher_id IS NULL OR shop_id IS NULL OR status NOT BETWEEN 1 AND 6
   OR original_amount < 0 OR pay_amount < 0 OR discount_amount < 0 OR refund_amount < 0
   OR pay_amount + discount_amount > original_amount OR refund_amount > pay_amount
   OR (status IN (2, 3, 5, 6) AND pay_time IS NULL) OR (status = 6 AND refund_time IS NULL);

CREATE TABLE quality_doris (
    check_time TIMESTAMP(3), check_name STRING, error_count BIGINT, sample_message STRING,
    PRIMARY KEY (check_time, check_name) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.ads_data_quality', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_quality_order', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

INSERT INTO quality_doris
SELECT quality_time, 'INVALID_ORDER_STATE', COUNT(*), MAX(CONCAT(error_reason, ':', CAST(id AS STRING)))
FROM invalid_order
GROUP BY quality_time;
