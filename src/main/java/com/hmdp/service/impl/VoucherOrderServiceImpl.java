package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.analytics.BehaviorEvent;
import com.hmdp.analytics.BehaviorEventPublisher;
import com.hmdp.analytics.BehaviorEventType;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherService voucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private BehaviorEventPublisher behaviorEventPublisher;
    @Lazy
    @Resource
    private IVoucherOrderService self;

    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    handleVoucherOrder(orderTasks.take());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("Failed to persist seckill order", e);
                }
            }
        });
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        RLock redisLock = redissonClient.getLock("lock:order:" + voucherOrder.getUserId());
        if (!redisLock.tryLock()) {
            log.warn("Duplicate asynchronous order ignored, userId={}, voucherId={}",
                    voucherOrder.getUserId(), voucherOrder.getVoucherId());
            return;
        }
        try {
            self.createVoucherOrder2(voucherOrder);
        } finally {
            redisLock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券不存在");
        }
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        if (!lock.tryLock()) {
            return Result.fail("不允许重复下单");
        }
        try {
            return self.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("用户已经购买过该优惠券");
        }
        boolean stockUpdated = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!stockUpdated) {
            return Result.fail("库存不足");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(redisIdWorker.nextId("order"));
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        fillOrderSnapshot(voucherOrder);
        save(voucherOrder);
        return Result.ok(voucherOrder.getId());
    }

    @Override
    public Result seckillVoucher2(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long scriptResult = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));

        if (scriptResult == null) {
            publishSeckillResult(voucherId, userId, orderId, "ERROR", "LUA_NO_RESULT");
            return Result.fail("秒杀服务暂时不可用");
        }
        int resultCode = scriptResult.intValue();
        if (resultCode != 0) {
            String reason = resultCode == 1 ? "OUT_OF_STOCK" : "DUPLICATE_ORDER";
            publishSeckillResult(voucherId, userId, orderId, "REJECTED", reason);
            return Result.fail(resultCode == 1 ? "库存不足" : "不能重复下单");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        if (!orderTasks.offer(voucherOrder)) {
            publishSeckillResult(voucherId, userId, orderId, "ERROR", "LOCAL_QUEUE_FULL");
            return Result.fail("订单队列繁忙，请稍后重试");
        }
        publishSeckillResult(voucherId, userId, orderId, "ACCEPTED", null);
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public void createVoucherOrder2(VoucherOrder voucherOrder) {
        int count = query()
                .eq("user_id", voucherOrder.getUserId())
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count();
        if (count > 0) {
            log.warn("Duplicate order ignored, userId={}, voucherId={}",
                    voucherOrder.getUserId(), voucherOrder.getVoucherId());
            return;
        }
        boolean stockUpdated = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!stockUpdated) {
            log.warn("Database stock is insufficient, voucherId={}", voucherOrder.getVoucherId());
            return;
        }
        fillOrderSnapshot(voucherOrder);
        save(voucherOrder);
    }

    @Override
    @Transactional
    public Result payOrder(Long orderId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getById(orderId);
        if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (order.getStatus() == null || order.getStatus() != 1) {
            return Result.fail("订单状态不允许支付");
        }
        boolean success = update()
                .set("status", 2)
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("user_id", userId)
                .eq("status", 1)
                .update();
        return success ? Result.ok(orderId) : Result.fail("订单状态已变更，请刷新后重试");
    }

    private void fillOrderSnapshot(VoucherOrder voucherOrder) {
        Voucher voucher = voucherService.getById(voucherOrder.getVoucherId());
        if (voucher == null) {
            throw new IllegalStateException("Voucher not found: " + voucherOrder.getVoucherId());
        }
        long payAmount = voucher.getPayValue() == null ? 0L : voucher.getPayValue();
        long originalAmount = voucher.getActualValue() == null ? payAmount : voucher.getActualValue();
        voucherOrder.setShopId(voucher.getShopId());
        voucherOrder.setOriginalAmount(originalAmount);
        voucherOrder.setPayAmount(payAmount);
        voucherOrder.setDiscountAmount(Math.max(originalAmount - payAmount, 0L));
        voucherOrder.setRefundAmount(0L);
    }

    private void publishSeckillResult(Long voucherId, Long userId, Long orderId,
                                      String result, String reason) {
        Voucher voucher = voucherService.getById(voucherId);
        behaviorEventPublisher.publish(BehaviorEvent.of(BehaviorEventType.SECKILL_REQUEST)
                .setUserId(userId)
                .setVoucherId(voucherId)
                .setOrderId(orderId)
                .setShopId(voucher == null ? null : voucher.getShopId())
                .setResult(result)
                .addProperty("reason", reason));
    }
}
