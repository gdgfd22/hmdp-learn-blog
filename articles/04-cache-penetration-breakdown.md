# 商户缓存治理：从 Cache Aside 到 Caffeine + Redis 二级缓存

> 摘要：商户详情是典型的高频读、低频写数据，直接读数据库既慢又压垮连接。本文记录我在 hm-dianping 改造项目里做的缓存治理：先用 Cache Aside 旁路缓存把读压力挪到 Redis，再针对缓存穿透、缓存击穿、缓存雪崩三类经典问题给出方案与取舍，最后叠加 Caffeine 本地一级缓存，并借助 Redis Pub/Sub 解决多实例的缓存失效同步。需要说明的是，这是本地单机环境下的个人学习实现，主链路只启用了空值缓存，互斥锁与逻辑过期是已经写出但未启用的备选代码。

## 一、为什么要这样做（业务背景与痛点）

商户详情页是本地生活应用里访问最频繁的页面之一：用户会反复查看同一家商户的图片、评分、营业时间，但商户资料本身很少被修改。这种「高频读、低频写」的数据，如果每次都直接查 MySQL，会面临三个问题：

- **查询延迟高**：一次详情查询要走索引、回表、网络往返，接口响应时间长；
- **数据库压力大**：请求量上来之后，连接池和磁盘 IO 先成为瓶颈；
- **热点被放大**：爆款商户的详情页可能被成千上万人同时访问，单表查询根本扛不住。

如果不加缓存，接口延迟和数据库负载会直接成为整个系统的短板。

## 二、用什么方法解决（方案对比）

缓存治理要先定「读写策略」，再逐个解决穿透、击穿、雪崩三类问题。

**1. 缓存更新：先删缓存还是先更新数据库？**

| 方案 | 做法 | 并发风险 |
| --- | --- | --- |
| 先删缓存，再更新数据库 | 更新前把缓存删掉 | 删除后、更新前，其他线程读到旧库并回填旧缓存，Redis 长期是旧值 |
| 先更新数据库，再删除缓存 | 数据库为新值后再删缓存 | 只在「更新完库、删除缓存」之间有极短的不一致窗口 |

项目采用**先更新数据库，再删除缓存**。「先删缓存」在读写并发下存在确定性漏洞：线程 A 删除缓存 → 线程 B 查库并写回旧数据 → 线程 A 更新数据库，最终 Redis 里永远是旧值；而「先更新库再删缓存」把不一致窗口压缩到极短，配合 Cache Aside 的读回填机制，最终能收敛到一致。

**2. 缓存穿透：参数校验 / 缓存空值 / 布隆过滤器**

| 方案 | 做法 | 代价 |
| --- | --- | --- |
| 参数校验 + 黑名单 | API 入口拦截非法参数、异常 IP | 只能挡住恶意请求，挡不住「合法但不存在」的 ID |
| 缓存空值 | 查不到就把空结果以短 TTL 缓存起来 | 多占用少量内存，实现简单 |
| 布隆过滤器 | 用概率结构先判断 key 是否存在 | 有误判率，需要提前构建合法 key 集合 |

项目当前数据量不大，选择**缓存空值**，实现最简单、收益直接。

**3. 缓存击穿：互斥锁 vs 逻辑过期**

| 方案 | 原理 | 优点 | 缺点 |
| --- | --- | --- | --- |
| 互斥锁 | 缓存失效时只让一个线程查库重建，其余等待或重试 | 数据一致 | 请求可能排队阻塞 |
| 逻辑过期 | 不设物理 TTL，过期后先返回旧值，后台线程异步重建 | 不阻塞、热点 key 不消失 | 短暂返回旧数据 |

**4. 缓存雪崩**：大量 key 同时失效或 Redis 故障导致流量直冲数据库，常规做法是随机 TTL、缓存预热、Redis 高可用与限流熔断，本项目在本地单机环境没有完整落地。

**5. 多实例失效**：Caffeine 是每个实例私有的本地缓存，实例之间无法自动同步，需要 Redis Pub/Sub 广播失效消息，详见第三部分。

## 三、为什么需要这个技术（原理深入）

### 3.1 Cache Aside：读写两条链路

读策略：先查 Redis，命中直接返回；未命中查 MySQL；有数据则回填 Redis；没数据则写空值防穿透。写策略：先更新数据库，再删除缓存。商户更新方法的实现非常直白：

```java
@Override
@Transactional
public Result update(Shop shop) {
    Long id = shop.getId();
    if (id == null) {
        return Result.fail("店铺id不能为空");
    }
    updateById(shop);
    stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
    return Result.ok();
}
```

### 3.2 空值缓存：TTL 设计

防穿透的关键在于「空值也要有过期时间」：二级缓存里正常数据 TTL 是 30 分钟，不存在的 key 用 2 秒的空值缓存。这样既能挡住大量不存在的 ID 打到数据库，又不会让空值长期占用内存。

```java
public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type,
        Function<ID,R> dbfallback, Long time, TimeUnit unit) {
    String key = keyPrefix + id;
    String json = stringRedisTemplate.opsForValue().get(key);
    if (StrUtil.isNotBlank(json)) {
        return JSONUtil.toBean(json, type);      // 命中数据，直接返回
    }
    if (json != null) {
        return null;                             // 命中空值，直接返回 null
    }
    R r = dbfallback.apply(id);                  // 未命中，查数据库
    if (r == null) {
        stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
        return r;                                // 查不到，写空值防穿透
    }
    this.set(key, r, time, unit);                // 查到，回填缓存
    return r;
}
```

### 3.3 互斥锁：缓存重建的节流阀

热点 key 失效的瞬间，大量请求同时回源会直接把数据库冲垮。互斥锁的思路是：只有拿到锁的线程去查库并重建缓存，其他线程短暂休眠后重试。

```java
private boolean tryLock(String key) {
    Boolean flag = stringRedisTemplate.opsForValue()
            .setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
    return Boolean.TRUE.equals(flag);
}

public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type,
        Function<ID, R> dbFallback, Long time, TimeUnit unit) {
    String lockKey = LOCK_SHOP_KEY + id;
    boolean isLock = tryLock(lockKey);           // SET NX EX 抢锁
    if (!isLock) {
        Thread.sleep(50);                        // 没抢到，休眠后重试
        return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
    }
    // 拿到锁：查库、重建缓存、释放锁
    ...
}
```

### 3.4 逻辑过期：热点数据不消失

逻辑过期不依赖 Redis 的物理过期时间，而是把业务数据和一个逻辑过期时间一起包进缓存 value（RedisData）。查询时发现逻辑过期，先返回旧数据，抢到锁的线程再开后台线程重建缓存——用户无感知，热点 key 也不会突然消失。

```java
public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
        Function<ID, R> dbFallback, Long time, TimeUnit unit) {
    String key = keyPrefix + id;
    String json = stringRedisTemplate.opsForValue().get(key);
    if (StrUtil.isBlank(json)) {
        return null;                             // 缓存不存在（正常只在预热阶段）
    }
    RedisData redisData = JSONUtil.toBean(json, RedisData.class);
    R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
    if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
        return r;                                // 未过期，直接返回
    }
    String lockKey = LOCK_SHOP_KEY + id;
    if (tryLock(lockKey)) {                      // 已过期：抢锁异步重建
        CACHE_REBUILD_EXECUTOR.submit(() -> {
            try {
                this.setWithLogicalExpire(key, dbFallback.apply(id), time, unit);
            } finally {
                unlock(lockKey);
            }
        });
    }
    return r;                                    // 先返回旧数据
}
```

### 3.5 Caffeine + Redis 二级缓存与多实例失效

Caffeine 是高性能的 JVM 本地缓存框架，直接访问本地内存、没有网络开销，比 Redis 更快；它使用 Window TinyLFU 淘汰算法，支持容量与时间过期策略。项目把查询链路拉长成三级：**Caffeine（L1）→ Redis（L2）→ MySQL**，命中下层就向上回填；L1 最多 1 万条、30 秒过期，L2 正常数据 30 分钟、空值 2 秒。

完整的数据流是：

- **查询**：先查 Caffeine，未命中再查 Redis，仍未命中查 MySQL，逐级向上回填；
- **更新**：数据库事务提交后，先清理本机 L1、删除 L2，再通过 Redis Pub/Sub 发布缓存失效消息，其他实例订阅后删除各自的本地缓存。

Caffeine 是实例私有的，不同实例间无法自动同步，所以 Pub/Sub 就是多实例一致性的通知通道。它延迟低、实现简单，但消息不持久，订阅者断线会漏消息，因此还要靠 L1 的 30 秒短 TTL 兜底：漏掉广播最坏也只是多服务 30 秒旧数据，最终收敛一致。

## 四、不用这个技术怎么办（替代方案与当前边界）

- **缓存更新**：追求强一致可以读写串行化或用版本号校验，但会牺牲吞吐；高并发读场景通常用 Cache Aside 配合重试、binlog 订阅或版本校验做最终一致。
- **防穿透**：数据量变大后可以升级为布隆过滤器，代价是要维护合法 key 集合，且存在误判率。
- **击穿**：互斥锁与逻辑过期可以二选一，也可以结合使用；注意逻辑过期适合允许短暂陈旧的数据（如商户详情），不适合库存、余额这类强一致数据。
- **多实例失效**：Pub/Sub 可以换成 RabbitMQ（可确认、可重试，但链路更复杂），或改用带版本号的缓存读取（读取时拒绝旧版本）。

**当前边界（本地单机学习项目）**：

1. 商户查询主链路只启用了空值缓存的 pass-through，它解决穿透，却没有解决热点 key 同时失效后的并发回源；互斥锁和逻辑过期有实现但未在主链路启用，不能说三个策略同时在线生效；
2. 练习版互斥锁是固定 value 加直接删除，不是完整的安全锁；未抢到锁时休眠 50 毫秒递归重试，缺少重试上限；
3. Pub/Sub 消息不持久，漏广播靠 30 秒 L1 TTL 兜底；L2 删除失败没有重试，旧值最坏可能持续到 30 分钟 TTL；
4. Caffeine 已开启 `recordStats()`，但还没有导出监控，因此无法给出命中率、性能提升等数字。

**生产升级路径**：先把互斥锁或逻辑过期接入主链路，并补上有界退避、超时与降级；L2 删除失败加重试或订阅 binlog 异步删除；失效通知换成可靠 MQ；最后用监控指标（Caffeine 命中率、Redis `keyspace_hits/misses`、接口 P95）驱动容量决策。

## 小结

1. 商户详情是高频读、低频写场景，Cache Aside 是缓存治理的地基：读时逐级回填，写时先更新数据库再删缓存。
2. 缓存穿透用空值缓存解决，空值必须配短 TTL，才能挡住无效请求又不浪费内存。
3. 缓存击穿是热点 key 失效瞬间的并发回源，互斥锁与逻辑过期是两种互补方案。
4. 缓存雪崩要靠随机 TTL、预热、高可用、限流熔断综合治理。
5. Caffeine 做一级缓存快但实例私有，Redis Pub/Sub 广播失效消息加短 TTL 兜底，构成最终一致的二级缓存。
6. 本地学习项目的边界要讲清楚：主链路目前只启用了空值缓存，互斥锁与逻辑过期仍是备选代码。
