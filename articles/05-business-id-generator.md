# 业务 ID 生成：基于雪花思想 + Redis 自增的全局唯一趋势递增方案

> 摘要：订单、评论、通知等场景都需要业务唯一 ID，直接依赖数据库自增主键会成为瓶颈。本文记录我在 hm-dianping 改造项目里实现的 RedisIdWorker：借鉴雪花算法「时间戳 + 序列」的拼接思想，用 Redis 的原子自增计数作为序列，生成全局唯一、趋势递增且生成延迟低的业务 ID。方案在本地单机环境验证，已应用于秒杀订单等核心场景。

## 一、为什么要这样做（业务背景与痛点）

在订单、评论、通知这些业务场景中，每条记录都需要一个业务唯一 ID。最简单的方式是直接使用数据库自增主键，但它在改造项目里会暴露几个问题：

- **数据库瓶颈**：自增 ID 的生成依赖数据库写入，高并发下单时，ID 生成本身会成为数据库上的一个热点；
- **扩展性差**：水平分库分表之后，多个库各自维护自增序列会撞号，必须额外设计步长或号段；
- **分布式部署受限**：多实例各自拿 ID 无法保证全局唯一，后续数据合并、迁移都会很痛苦。

秒杀场景尤其典型：请求先要在 Redis 侧完成库存与资格校验、生成订单 ID，随后才异步落库。如果订单 ID 依赖数据库自增，就等于把「高并发入口」和「数据库」强绑定在一起，异步削峰的意义就打了折扣。因此项目实现了一套基于 Redis 自增计数器和时间戳拼接的业务 ID 生成方案。

## 二、用什么方法解决（方案对比）

| 方案 | 全局唯一 | 趋势递增 | 生成延迟 | 主要问题 |
| --- | --- | --- | --- | --- |
| 数据库自增 | 单库内唯一 | 是 | 依赖数据库写入 | 分库分表撞号、数据库热点 |
| UUID | 是 | 否 | 低 | 无序、太长，索引局部性差 |
| 经典雪花 | 是 | 是 | 低 | 依赖机器 ID，需处理时钟回拨 |
| 纯 Redis 自增 | 是 | 是 | 低 | 只有序列，ID 不含时间维度 |
| 本项目：时间戳 + Redis 自增 | 是 | 是 | 低 | 依赖 Redis 可用性 |

项目最终选择「雪花思想的拼接 + Redis 原子自增」：ID 由高位时间戳和低位自增序列拼成，高位保证趋势递增，低位保证同一时刻内不重复；自增交给 Redis 的 INCR，天然原子，多实例共享同一个 Redis 时天然全局唯一。

## 三、为什么需要这个技术（原理深入）

### 3.1 雪花思想：时间戳 + 序列拼接

经典雪花算法把 64 位拆成「时间戳 + 机器 ID + 序列」三段；本项目把它简化成「时间戳 + 序列」两段：`timestamp << 32 | count`。时间戳取自「当前秒 - 固定起点秒」，保证 ID 随时间是趋势递增的；低位 32 位是当天该业务 key 下的自增计数，保证同一秒内即使并发多次也不会重复。高位时间戳决定大小走向，低位序列保证唯一性，两者一拼就是「全局唯一 + 趋势递增」：

```java
@Component
public class RedisIdWorker {
    private static final long BEGIN_TIMESTAMP = 1640995200L;  // 2022-01-01 作为起点
    private static final int COUNT_BITS = 32;

    public long nextId(String keyPrefix) {
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;          // 距起点的秒数

        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue()
                .increment("icr:" + keyPrefix + ":" + date);  // 按天自增

        return timestamp << COUNT_BITS | count;               // 拼接成 64 位 ID
    }
}
```

### 3.2 数据流：按天 key + 原子 INCR

每次调用 `nextId` 的数据流是：

1. 取本地时钟的当前秒，减去固定起点秒，得到时间戳；
2. 用「日期 + 业务前缀」拼出 Redis key（例如 `icr:order:2025:01:12`），执行 INCR 拿到当天的自增计数；
3. 时间戳左移 32 位，与计数按位或，得到 64 位 long 型 ID。

按天拆 key 有两个好处：一是计数每天从 1 重新开始，单个 key 不会无限增长；二是日期信息留在 key 里，方便按日排查。Redis 的 INCR 是单命令原子操作，天然解决并发自增的竞争问题，比「先查再写」的 Java 代码更可靠。

### 3.3 业务侧使用：秒杀订单

秒杀下单入口先生成订单 ID，再把它交给 Lua 脚本做库存与一人一单校验：

```java
public Result seckillVoucher2(Long voucherId) {
    Long userId = UserHolder.getUser().getId();
    long orderId = redisIdWorker.nextId("order");  // 先生成业务订单 ID

    Long result = stringRedisTemplate.execute(
            SECKILL_SCRIPT, Collections.emptyList(),
            voucherId.toString(), userId.toString(), String.valueOf(orderId)
    );
    int r = result.intValue();
    if (r != 0) {
        return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
    }
    // 校验通过：写入本地队列，异步落库
    ...
}
```

这里 orderId 在 Lua 校验之前生成：即使请求最终被拒，ID 也不会冲突；异步落库时订单主键直接使用这个 ID，无需再回数据库取一次自增值。

## 四、不用这个技术怎么办（替代方案与当前边界）

- **数据库自增**：实现最简单，但分库分表要设计步长或号段，且 ID 生成与数据库写入强耦合；
- **UUID**：本地生成、零依赖，但无序且过长，作为主键会伤害索引的局部性，也不适合暴露在 URL 里；
- **经典雪花**：需要管理机器 ID，还要处理时钟回拨（常见做法是记录上次生成的时间戳，回拨时等待或直接报错）；本项目不引入机器 ID 段，但本地时钟同样可能回拨，方案里没有做防护；
- **纯 Redis 自增**：也能全局唯一，但只有序列、没有时间维度，ID 无法反映时间、规律容易被探测，一般要配合时间戳使用。

**当前边界（本地单机学习项目）**：

1. 计数依赖本地单机 Redis，Redis 不可用时 ID 生成不可用，没有降级方案；
2. 时间戳来自本地时钟且未做回拨防护；32 位序列每天上限约 42.9 亿，单日超过会溢出（本地场景远达不到这个量级）；
3. 时间戳基于 UTC 秒计算，跨时区部署时要统一起点的含义；
4. 秒杀中 orderId 在 Lua 之前生成，若后续改造成可靠消息队列，消息要携带 messageId、orderId、userId、voucherId，并以 orderId 做数据库幂等；
5. 当前脚本在单机 Redis 上运行；Redis Cluster 下 Lua 多 key 必须落在同一 hash slot（可用相同 hash tag 的 key 设计），不能直接宣称兼容 Cluster。

**生产升级路径**：可以换成经典雪花（补机器 ID 段与时钟回拨处理），或采用号段模式（如 Leaf segment，批量取号、数据库落点、本地生成），也可以保留 Redis 自增思路但补齐可用性降级与容量监控。

## 小结

1. 业务 ID 不能依赖数据库自增：分库分表会撞号、数据库会热点化、分布式部署受限。
2. 本项目借鉴雪花思想，用「时间戳 + Redis 原子自增序列」拼接出 64 位 long 型 ID，实现更轻。
3. 高位时间戳保证趋势递增，低位序列按天 key 自增保证唯一，INCR 天然原子、跨实例共享。
4. 按天拆 key 让计数每天归零，key 不膨胀，日期信息还留在 key 里便于排查。
5. 秒杀场景先由 RedisIdWorker 生成 orderId，再进 Lua 校验，ID 生成与数据库完全解耦。
6. 边界要讲清楚：单机 Redis、无时钟回拨防护、32 位序列有日上限；生产升级可走经典雪花或号段模式。
