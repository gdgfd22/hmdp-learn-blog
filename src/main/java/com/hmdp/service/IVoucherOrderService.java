package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {
    public Result seckillVoucher(Long voucherId);

    public Result createVoucherOrder(Long voucherId);

    public Result seckillVoucher2(Long voucherId);

    Result payOrder(Long orderId);

    void createVoucherOrder2(VoucherOrder voucherOrder);
}
