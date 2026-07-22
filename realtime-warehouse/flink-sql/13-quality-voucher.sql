SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'hmdp-quality-invalid-voucher';

CREATE TABLE mysql_voucher_cdc (
    id BIGINT, shop_id BIGINT, pay_value BIGINT, actual_value BIGINT, `type` INT, status INT,
    update_time TIMESTAMP(3),
    quality_time AS CAST(DATE_FORMAT(update_time, 'yyyy-MM-dd HH:mm:00') AS TIMESTAMP(3)),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc', 'hostname' = 'mysql', 'port' = '3306',
    'username' = 'flink', 'password' = 'flink_pw', 'database-name' = 'hmdp',
    'table-name' = 'tb_voucher', 'server-time-zone' = 'Asia/Shanghai',
    'server-id' = '5431-5434', 'scan.startup.mode' = 'initial'
);

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

CREATE TABLE quality_doris (
    check_time TIMESTAMP(3), check_name STRING, error_count BIGINT, sample_message STRING,
    PRIMARY KEY (check_time, check_name) NOT ENFORCED
) WITH (
    'connector' = 'doris', 'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'hmdp_analytics.ads_data_quality', 'username' = 'root', 'password' = '',
    'sink.label-prefix' = 'hmdp_quality_voucher', 'sink.enable-2pc' = 'true',
    'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'
);

INSERT INTO quality_doris
SELECT quality_time, 'INVALID_VOUCHER_STATE', COUNT(*), MAX(CONCAT(error_reason, ':', CAST(id AS STRING)))
FROM invalid_voucher
GROUP BY quality_time;
