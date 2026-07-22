-- Run once against an existing hmdp database. New Docker databases already include these columns.
ALTER TABLE tb_voucher_order
    ADD COLUMN shop_id BIGINT UNSIGNED NULL AFTER voucher_id,
    ADD COLUMN original_amount BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER shop_id,
    ADD COLUMN pay_amount BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER original_amount,
    ADD COLUMN discount_amount BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER pay_amount,
    ADD COLUMN refund_amount BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER discount_amount,
    ADD UNIQUE KEY uk_user_voucher (user_id, voucher_id),
    ADD KEY idx_shop_create_time (shop_id, create_time);

UPDATE tb_voucher_order o
JOIN tb_voucher v ON o.voucher_id = v.id
SET o.shop_id = v.shop_id,
    o.original_amount = v.actual_value,
    o.pay_amount = v.pay_value,
    o.discount_amount = GREATEST(v.actual_value - v.pay_value, 0)
WHERE o.shop_id IS NULL;

ALTER TABLE tb_voucher_order MODIFY COLUMN shop_id BIGINT UNSIGNED NOT NULL;
