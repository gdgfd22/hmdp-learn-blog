# 优惠券秒杀：从超卖问题到 Redis Lua + Redisson 分布式锁 + 异步削峰的完整链路

> 摘要：秒杀是典型的高并发场景：券数量固定、单人限购、流量集中，直接操作数据库会依次踩中"超卖""一人多单""写压力过载"三个坑。本文沿着踩坑路线展开：从"查库存再扣库存"的超卖，到 synchronized 解决一人一单、事务与锁生命周期错位、内部调用事务失效，最终落地为 Lua 原子预检 + Redisson 分布式锁 + 条件更新 + 异步削峰的完整链路，并讲清替代方案与当前边界——它只是本地单机环境的"异步削峰验证"，还不是可靠的 MQ 链路。

## 一、为什么要这样做（业务背景与痛点）

优惠券秒杀的业务特征很极端：券数量固定、抢购时间高度集中、流量在开售瞬间打到峰值。课程版本的实现是"先查库存、再扣库存、再下单"，低并发下没问题，并发一起来就会暴露三个问题：

1. **超卖**：库存只剩 1 时，两个线程同时执行"判断库存 > 0"，都通过，然后各自扣减，最终库存变成 -1。根因是"判断 + 扣减"不是原子操作，中间一旦被打断就会重复扣。
2. **一人多单**：两个线程同时查询"该用户是否已购买"，都查到 count=0，然后各自插入订单，同一用户最终两条订单。根因是"查询 + 判断 + 插入"也不是原子操作。
3. **数据库瞬时写压力**：每个请求都要到 MySQL 查询和写入，开售瞬间所有流量直打数据库，连接池被打满会拖垮整个服务而不只是秒杀接口。

不解决会怎样：超卖让库存扣成负数、活动方兜底资损；一人多单破坏公平；数据库被打挂则整个平台不可用。所以秒杀目标非常明确：**不能超卖、一人只能买一次、扛得住高并发**。

## 二、用什么方法解决（方案对比）

| 问题 | 候选方案 | 本项目选择 | 选择理由 |
|---|---|---|---|
| 防超卖 | 悲观锁（`select for update`）/ version 乐观锁 / 原子 SQL | 原子 SQL 条件更新 | "判断 + 扣减"合并成一条 SQL，数据库保证原子性，无需额外字段、无锁竞争 |
| 一人一单 | `synchronized`（JVM 锁）/ Redis 分布式锁 | Redisson 分布式锁 | synchronized 只能保证单 JVM 内互斥，集群下各 JVM 常量池互不可见，需要跨 JVM 的锁 |
| 削峰 | 本地阻塞队列 / Redis Stream / RabbitMQ | 本地阻塞队列（验证阶段） | 实现最简单，先验证"前置校验 + 快速返回 + 异步落库"主链路，后续再升级 |

防超卖这里有个经验点：乐观锁（version 字段 CAS）适合"更新已有数据"；而一人一单是"插入新数据"，没有可比较的旧版本，所以插入侧要改用锁来串行化。单机环境 `synchronized(userId.toString().intern())` 够用——`intern()` 从字符串常量池取唯一对象；但集群下每个 JVM 各有常量池，`synchronized` 无法保证多 JVM 之间的互斥，必须上分布式锁。

## 三、为什么需要这个技术（原理深入）

### 3.1 先看完整链路

一条秒杀请求最终走的是：

```
请求进入 → 生成订单 ID（Redis 业务 ID）
→ Redis Lua 原子校验：查库存、查资格、扣库存、记资格（预扣）
→ offer 写入本地有界队列，接口立即返回（只表示"受理"，不是"成功"）
→ 单线程消费者取出任务
→ 加用户维度 Redisson 分布式锁
→ 经 Spring 代理执行数据库事务：条件扣库存（stock > 0）防超卖 + 插入订单（唯一索引兜底）
→ finally 中释放锁
```

这条链路把"校验 + 预扣"放到 Redis 做前置挡板，把"落库"交给异步消费者，请求线程不再直接写库，这就是削峰。

### 3.2 Lua 脚本为什么是原子的

```java
@Override
public Result seckillVoucher2(Long voucherId) {
    Long userId = UserHolder.getUser().getId();
    long orderId = redisIdWorker.nextId("order");

    Long result = stringRedisTemplate.execute(
            SECKILL_SCRIPT,
            Collections.emptyList(),
            voucherId.toString(), userId.toString(), String.valueOf(orderId)
    );

    int r = result.intValue();
    if (r != 0) {
        return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
    }
    // ...（返回 0 后：构造订单任务入队，接口返回受理）
}
```

Redis 单线程串行处理命令，Lua 脚本会被当成"一条命令"整体执行，期间其他命令不能插入，所以"查库存、查资格、扣库存、记资格"不会被并发打断。但原子性不是回滚机制：脚本中途报错不会撤销已执行的写命令，所以必须先校验、后写入。另外 Redis Cluster 要求脚本访问的所有 Key 落在同一 hash slot 并应通过 `KEYS` 传入；当前脚本动态拼 Key 且跑在单机 Redis 上，不能宣称兼容 Cluster。

### 3.3 事务与锁的生命周期、AopContext 代理

初版代码把 `synchronized` 写在 `@Transactional` 方法内部，结果仍然重复下单：

```
线程 A：save() → 释放锁 → 事务还没提交
线程 B：拿到锁 → 查数据库看不到 A 的数据（未提交）→ 再次插入
```

原因在于 Spring 事务基于 AOP 代理实现，事务提交发生在"方法执行结束之后"，而锁在"synchronized 块结束时"就释放了，**锁的生命周期 < 事务的生命周期**。正确做法是锁在外层方法、事务方法放内层：

```java
public Result seckillVoucher() {
    synchronized (...) {
        return this.createVoucherOrder();
    }
}

@Transactional
public Result createVoucherOrder() { ... }
```

但 `this.createVoucherOrder()` 又引入了新问题：Spring 事务基于代理，**内部调用不会经过代理**，事务不生效。正确方式是显式获取代理对象再调用：

```java
synchronized (...) {
    // 注意：代理对象是接口类型
    IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
    return proxy.createVoucherOrder(voucherId);
}
```

并在启动类开启 `@EnableAspectJAutoProxy(exposeProxy = true)` 暴露代理。

### 3.4 Redisson 分布式锁：watchdog、可重入、正确解锁

```java
private void handleVoucherOrder(VoucherOrder voucherOrder) {
    Long userId = voucherOrder.getUserId();
    RLock redisLock = redissonClient.getLock("lock:order:" + userId);
    boolean isLock = redisLock.tryLock();
    if (!isLock) {
        log.error("不允许重复下单！");
        return;
    }
    try {
        proxy.createVoucherOrder2(voucherOrder);
    } finally {
        redisLock.unlock();
    }
}
```

项目调用的是未指定 leaseTime 的 `tryLock()`，成功后 Redisson 会启用 watchdog：默认给锁约 30 秒 TTL，并按租期约三分之一周期自动续期，避免业务没执行完锁就过期。Redisson 以"客户端 ID + 线程 ID"标识持有者、用重入计数支持可重入；解锁必须在同一线程的 `finally` 中执行。但 watchdog 仍可能受长 GC、网络中断、主从切换影响，分布式锁不能替代数据库层幂等兜底。

### 3.5 数据库条件更新：最后的兜底

```java
// 事务方法内部：先查一人一单，再条件扣库存，最后插入订单
int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
if (count > 0) {
    return Result.fail("用户已经购买过一次！");
}
boolean success = seckillVoucherService.update()
        .setSql("stock = stock - 1")          // set stock = stock - 1
        .eq("voucher_id", voucherId)          // where voucher_id = ?
        .gt("stock", 0)                       // and stock > 0
        .update();
if (!success) {
    return Result.fail("库存不足！");
}
```

`voucher_id` 是秒杀券表主键，等值更新能定位到具体索引记录并加排他记录锁；`stock > 0` 作为条件把"判断 + 修改"合并成单条原子更新，受影响行数为 0 就表示库存不足，数据库不会把库存扣成负数。它没有 version 字段，不是经典乐观锁，但属于"基于条件更新的乐观并发控制"：先尝试原子修改，再根据受影响行数判断竞争是否成功。若条件无法命中索引，InnoDB 会扫描并锁住大量记录，效果接近锁全表。

### 3.6 唯一索引：一人一单的最终硬兜底

代码里的 `count > 0` 是"先查再插"的**软校验**，并发下两个线程可能同时查到 0 再同时插入，它挡不住竞态。真正兜底"一人一单"的是订单表上的唯一索引：

```sql
CREATE TABLE `tb_voucher_order` (
  `id` bigint(20) NOT NULL COMMENT '主键',
  `user_id` bigint(20) UNSIGNED NOT NULL,
  `voucher_id` bigint(20) UNSIGNED NOT NULL,
  ...
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_user_voucher` (`user_id`, `voucher_id`) USING BTREE
) ENGINE = InnoDB;
```

业务软校验与存储引擎硬约束的职责完全不同：

| 校验 | 位置 | 性质 | 可靠性 |
|---|---|---|---|
| `count > 0` 先查再插 | 业务代码（事务内） | 软校验 | 并发下可同时通过，挡不住竞态 |
| `uk_user_voucher` 唯一索引 | InnoDB 存储引擎 | 硬约束 | 数据库层面强制，无法绕过 |

唯一索引的冲突检测发生在 **INSERT 语句执行时**，由 InnoDB 内部完成，与业务代码查不查 count 无关。即使 count 被并发绕过，第二个线程的 INSERT 也会撞上唯一索引直接失败——这就是"最终兜底"的含义。

InnoDB 保证唯一性的原理：唯一索引是一棵独立的二级索引 B+Tree，叶子节点按 `(user_id, voucher_id)` 排序存储。INSERT 时在树中按序定位插入位置，重复判断就是一次 O(log n) 的树查找。并发插入同键时靠锁裁决：事务 A 插入 `(1, 100)` 成功并持有记录锁（未提交），事务 B 插入同键会在查找时发现 A 的记录并被 A 的锁阻塞；A 提交释放锁后 B 才醒来，确认键值重复，返回 1062 Duplicate entry。所以"唯一"不是碰运气，而是先到者持锁、后到者被锁挡住，最终只有一个事务能插进去。

两个使用要点：

1. **唯一索引对 NULL 失效**：MySQL 里 `NULL != NULL`，所以唯一索引允许多个 NULL 值。若 `user_id` 或 `voucher_id` 允许为 NULL，`(NULL, 100)` 可以插无数次，约束形同虚设。表结构里两个字段都必须是 `NOT NULL`——面试被问"唯一索引为什么没生效"，十有八九是字段可空或没走索引。
2. **重复插入要按正常业务分支处理**，而不是让异常变成 500：

```java
try {
    save(voucherOrder);
} catch (DuplicateKeyException e) {   // 1062 Duplicate entry
    log.warn("重复下单，userId={}, voucherId={}", userId, voucherId);
    return Result.fail("您已购买过该优惠券");
}
```

完整防重复的纵深防线：**Redis Lua 前置挡板（拦掉绝大多数并发重复）→ 用户维度分布式锁（串行化同一用户下单）→ 事务内 count 软校验（减少无谓扣库存）→ 条件扣库存 `stock > 0`（防超卖）→ `uk_user_voucher` 唯一索引（最终硬兜底）→ 捕获 DuplicateKeyException 按正常分支处理**。每一层都可能被绕过，但唯一索引这层不可能被绕过——这也是升级为 RabbitMQ 可靠消息化后，消息重复投递时保证消费幂等的那道最终防线。

### 3.7 异步削峰：阻塞队列 + 单线程消费者

```java
private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

@PostConstruct
private void init() {
    SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
}
```

入队用 `offer`：队列满时立即返回 `false`，不会长期占住请求线程（对比：`add` 满时抛异常，`put` 阻塞等待）。单线程消费者取任务后执行上文加锁 + 代理事务调用，天然串行。问题随之而来：Lua 预扣成功后 `offer` 失败没有补偿，用户被占库存且无法重试；`tryLock` 失败当前也直接丢弃任务；容量虽有界，一百多万条对象仍有明显内存风险。这就是"本地异步削峰验证"的含义。

## 四、不用这个技术怎么办（替代方案与当前边界）

**不用 Lua**：单机用 synchronized 保证"get + 判断 + delete"原子；基于 Redis 可用 WATCH + MULTI/EXEC 实现 CAS；或用数据库唯一索引、行锁做分布式锁。但实际项目里释放分布式锁最常用、最推荐的仍是 Lua 脚本。

**不用 Redisson**：单机用 synchronized；分布式场景自己用 setnx 加锁、再手动续期和解锁很容易出 bug（误删别人的锁、锁过期业务未结束），Redisson 把 watchdog、可重入、正确解锁都封装好了。

**不用本地队列**：升级方向有两条——Redis Stream 更适合承接秒杀核心异步下单链路（已有 Redis、可 ACK）；RabbitMQ 更适合秒杀后的通知、埋点、结果分发等扩展链路。为什么不是 Kafka？异步下单是业务命令，更需要 ACK、未确认任务、重试、死信和补偿；Kafka 更擅长高吞吐事件日志和回放，本项目已把它用在分析事件流上，职责更清晰。

**当前边界（诚实口径）**：这是个人学习项目，全部在本地单机/容器环境验证。秒杀当前是本地队列方案：队列不持久化，进程宕机会丢失该 JVM 中尚未落库的任务，而 Redis 里的资格和库存仍保留；多实例部署时各有独立队列，无法统一查看积压；`offer`、抢锁、落库失败都没有补偿、重试和状态查询；Redis 的预扣和资格记录不在本地事务内，扣库存成功但插订单失败时 MySQL 回滚、Redis 预扣不会跟着回滚——跨存储一致性尚未补齐；入口也未校验活动起止时间；按 userId 加锁会把同一用户购买不同券也串行化，粒度偏粗，生产可改为 `userId + voucherId` 并保留唯一索引兜底。

**生产环境如何升级**：改为可靠的 RabbitMQ 链路——入口先生成 orderId，Lua 原子完成库存、资格校验和预占；成功后构造带 `messageId、orderId、userId、voucherId` 的订单命令，持久化发送到 durable direct exchange 和订单队列，生产端开启 publisher confirm 与 mandatory/return；消费者以手动 ACK 在同一个 MySQL 事务里按 orderId 幂等校验、`stock > 0` 条件扣减并插入订单，事务提交后才 ACK。临时异常进入有次数上限的延迟重试，超阈值的任务进死信队列并告警；确认无成功订单后，用带 orderId 和状态 CAS 的幂等 Lua 恢复 Redis 库存、删除资格，并提供 `PENDING/SUCCESS/FAILED` 状态查询。也要承认：RabbitMQ 只解决任务持久化投递，不能保证"Lua 预扣 + 发送"原子完成——即使开启 confirm，Lua 成功后应用宕机、消息尚未发送的窗口依然存在，需要预占记录 + 定时扫描对账；若更强调入队原子性，可让 Lua 同脚本 `XADD` 到 Redis Stream，或改用数据库 Outbox（需重设计削峰与库存口径）。

## 小结

- 超卖的本质是"判断 + 扣减"不原子，用 `stock = stock - 1 ... where stock > 0` 条件更新把判断和修改合并成一条 SQL 解决。
- 一人一单要先解决"锁的粒度"（按用户维度），再解决"锁生命周期 < 事务生命周期"（锁在外层、事务在内层），再解决"内部调用不走代理"（AopContext 取代理）。
- 集群下 JVM 锁失效，需要 Redisson 分布式锁；watchdog、可重入、finally 解锁是正确姿势，但它不能替代数据库幂等。
- Lua 脚本在 Redis 中整体原子执行，但原子不等于回滚，脚本要先校验后写入。
- 异步削峰让接口"受理即返回"，落库交给单线程消费者；本地队列的代价是不持久、不重试、宕机丢任务，只能叫本地异步削峰验证。
- 生产化方向是 Redis Stream 或 RabbitMQ 可靠消息链路，并补预占记录、对账、状态查询与幂等恢复。
