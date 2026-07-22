package com.hmdp.controller;


import com.hmdp.analytics.BehaviorEvent;
import com.hmdp.analytics.BehaviorEventPublisher;
import com.hmdp.analytics.BehaviorEventType;
import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IVoucherService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher")
public class VoucherController {

    @Resource
    private IVoucherService voucherService;

    @Resource
    private BehaviorEventPublisher behaviorEventPublisher;

    /**
     * 新增普通券
     * @param voucher 优惠券信息
     * @return 优惠券id
     */
    @PostMapping
    public Result addVoucher(@RequestBody Voucher voucher) {
        voucherService.save(voucher);
        return Result.ok(voucher.getId());
    }

    /**
     * 新增秒杀券
     * @param voucher 优惠券信息，包含秒杀信息
     * @return 优惠券id
     */
    @PostMapping("seckill")
    public Result addSeckillVoucher(@RequestBody Voucher voucher) {
        voucherService.addSeckillVoucher(voucher);
        return Result.ok(voucher.getId());
    }

    /**
     * 查询店铺的优惠券列表
     * @param shopId 店铺id
     * @return 优惠券列表
     */
    @GetMapping("/list/{shopId}")
    public Result queryVoucherOfShop(@PathVariable("shopId") Long shopId, HttpServletRequest request) {
        Result result = voucherService.queryVoucherOfShop(shopId);
        if (Boolean.TRUE.equals(result.getSuccess()) && result.getData() instanceof List) {
            for (Object item : (List<?>) result.getData()) {
                if (item instanceof Voucher) {
                    Voucher voucher = (Voucher) item;
                    behaviorEventPublisher.publish(BehaviorEvent.of(BehaviorEventType.VOUCHER_EXPOSURE)
                            .setShopId(shopId)
                            .setVoucherId(voucher.getId())
                            .setDeviceId(request.getHeader("X-Device-Id")));
                }
            }
        }
        return result;
    }
}
