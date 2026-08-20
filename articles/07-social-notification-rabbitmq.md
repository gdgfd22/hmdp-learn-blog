# 社交模块：关注 Feed（Set/ZSet、推模式、滚动分页）与 RabbitMQ 异步通知中心

> 摘要：社交模块有两件核心事：一是让用户看到关注对象的动态（Feed），二是把点赞、评论、关注产生的通知送达对方。前者用 Redis Set/ZSet 承载关注关系与时间线，采用纯推模式 + `max + offset` 滚动分页；后者通过 RabbitMQ 异步化，在事务提交后投递消息，由消费者统一落库并构建通知中心。本文讲清数据结构选型、滚动分页的边界细节、afterCommit 与 Outbox、消费幂等，以及当前实现还有哪些缺口——它是本地容器环境下的学习验证，不是生产级消息链路。

## 一、为什么要这样做（业务背景与痛点）

博客、点赞、评论、关注是社交模块的基本动作。它们带来两类痛点：

1. **通知逻辑让主链路变长、耦合升高**。如果点赞、评论、关注时同步处理"给被通知人落一条通知"，业务代码要和通知逻辑、通知中心构建纠缠在一起：一个点赞接口既要改点赞状态，又要写通知表，还要处理已读未读。每加一种新通知类型，所有业务方法都要改一遍，主链路响应时间也被拖长。
2. **Feed 读取的成本被放大了**。用户打开首页要看到"我关注的人发布的博客"。最朴素的实现是每次读取时遍历我的关注列表、逐个查数据库再合并排序；关注的人越多、每人的博客越多，单次读取的数据库查询次数就线性膨胀，热点用户下数据库压力很大。

不解决会怎样：通知逻辑散落在各个业务方法里，接口变慢、代码耦合、难以扩展新类型；Feed 用"读时全量合并"的方式做，普通用户也会越用越卡，数据库被打爆。所以社交模块的目标是：**Feed 读得快、通知与主链路解耦**。

## 二、用什么方法解决（方案对比）

| 关注点 | 候选方案 | 本项目选择 | 选择理由 |
|---|---|---|---|
| 关注关系存储 | 只存 MySQL / MySQL + Redis Set | MySQL 落事实 + Redis Set 支持共同关注 | 关注关系以 MySQL 为准，Set 提供 `SINTER` 交集能力 |
| Feed 模式 | 拉模式 / 推模式 / 推拉结合 | 纯推模式 | 粉丝规模不大时发布成本可接受，读取最快 |
| Feed 数据结构 | List / ZSet | ZSet（member 为博客 ID，score 为时间） | 天然按时间排序，支持范围分页 |
| 分页方式 | 页码分页 / `max + offset` 滚动分页 | 滚动分页 | 以最小 score 为游标，能抵抗新 Feed 插入导致的翻页错乱 |
| 通知处理 | 同步写库 / 线程池异步 / 本地队列 / MQ | RabbitMQ 异步 + 通知中心 | durable 队列 + persistent 消息，路由与失败处理成熟 |

Feed 模式的选择值得展开：**推模式**在博主发布时把内容写到每个粉丝的收件箱（写时扩散），读的时候一次 Redis 范围查询就拿到一页，普通用户读取极快，代价是发布成本与粉丝数成正比，大 V 发布会造成写放大；**拉模式**读时合并，发布成本低但对大 V 友好、普通用户读取慢；**推拉结合**则对普通用户继续推、对大 V 改为读时拉取。本项目受众是粉丝规模不大的普通用户，所以选纯推模式，同时知道大 V 场景需要升级为推拉结合并设置收件箱容量与过期策略。

## 三、为什么需要这个技术（原理深入）

### 3.1 ZSet：score 就是时间戳

Feed 的核心需求是"按时间倒序、按页取"。Redis ZSet 的 member 存博客唯一 ID，score 存发布时间戳，天然支持按 score 范围取数据；Set 则用来存关注关系，共同关注用 `SINTER` 一次求交。注意 `SINTER` 的风险：它适合中小关注集合，但大集合求交会占用 Redis 主线程并产生大结果，规模扩大后要限制集合规模与返回量、离线预计算，或改用数据库/图存储，不能放在请求链路里无界返回。

### 3.2 滚动分页：`max + offset` 为什么这样设计

Feed 不断有新内容插入，用页码分页（offset 固定）会在翻页时出现重复或漏读。滚动分页的正确思路是：下一页把上一页的最小 score 作为新的 `max` 上限；如果上一页边界上仍是同一个 score（同分数据多于一页），就把本次同分条数与旧 offset 累加，否则从本次同分条数重新计数；member 用唯一博客 ID 去重。**但当前源码把这个"同分 offset 分支"写反了**：会在全同分时重复数据、跨到新 score 时多跳，修复前可能重复或漏数据——这是必须承认的边界 bug，面试前应修复并补边界测试。

### 3.3 通知投递：事务提交后再发送

点赞、关注、评论三个业务方法接入统一的通知发布入口：

```java
public void publishAfterCommit(SocialNotificationMessage message) {
    if (TransactionSynchronizationManager.isActualTransactionActive()) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                send(message);
            }
        });
        return;
    }
    send(message);
}
```

`afterCommit` 的意义是：把"发消息"注册为事务提交后的回调，业务事务回滚时不会误发通知。但要知道它的边界：它只能避免"事务回滚后误发"，**数据库已经提交、回调尚未发出时进程宕机，消息仍然会丢**；而且点赞和关注方法当前并非都处于事务中，有些通知会在数据库操作后直接发送。

### 3.4 消费者统一落库与通知中心

```java
@RabbitListener(queues = MqConstants.SOCIAL_NOTIFICATION_QUEUE)
public void handleSocialNotification(SocialNotificationMessage message) {
    Notification notification = new Notification();
    notification.setUserId(message.getReceiverUserId());
    notification.setSenderUserId(message.getSenderUserId());
    notification.setType(message.getType());
    notification.setBizId(message.getBizId());
    notification.setContent(message.getContent());
    notification.setReadStatus(NotificationConstants.READ_UNREAD);
    notificationService.save(notification);
}
```

消息经 durable direct exchange 按 routing key 路由到 durable queue，消息默认 persistent，队列重启不丢。消费者统一把通知落库，业务侧完全不用感知"给谁发、怎么写通知表"。通知中心提供三类能力：查询通知列表、查询未读数、标记已读：

```java
@Override
public Result countUnread() {
    Long userId = UserHolder.getUser().getId();
    int count = query()
            .eq("user_id", userId)
            .eq("read_status", NotificationConstants.READ_UNREAD)
            .count();
    return Result.ok(count);
}
```

### 3.5 消费幂等与 Outbox

RabbitMQ 投递是"至少一次"语义：消费者处理成功但 ACK 前宕机，Broker 会重新投递，于是必然可能出现重复消息。把业务效果收敛为一次，要靠数据库唯一约束或幂等表。**当前通知表只有普通索引**，没有基于"接收人 + 类型 + 业务 ID"的唯一约束，消费者也没有幂等判断，重复消息可能插入重复通知——这是当前缺口。生产上更完整的做法是 Outbox：在同一个数据库事务里同时写业务数据和 outbox 事件，独立投递器（或 CDC）读取未发送事件投递 MQ、确认后更新发送状态；即使投递器宕机，事件记录仍可重试，消费者再用业务唯一键保证幂等，从而形成最终一致闭环。

### 3.6 为什么通知用 RabbitMQ、行为流用 Kafka

这不是重复堆技术：通知是低到中吞吐的业务消息，重视路由、ACK、重试和死信；行为日志是高吞吐、可回放的事件流，要供多个 Flink 作业用不同消费组独立读取，更适合 Kafka。二者服务的是不同语义，本项目也确实是这么分工的。

## 四、不用这个技术怎么办（替代方案与当前边界）

**Feed 不用推模式**：可以改拉模式——每次读取时合并关注列表，发布成本为零，但普通用户读取变慢，热点用户下数据库压力大；也可以做推拉结合，对普通用户推、对大 V 拉，配合收件箱容量限制与过期策略。

**通知不用 MQ**：同步写通知表最简单，但主链路变长、耦合升高，每加一种通知类型都要改业务方法；用线程池异步能缓解响应时间，但没有持久化，进程重启消息就丢了；本地队列同样如此。MQ 的价值在于持久化、ACK、重试、死信这些可靠投递能力，代价是多一个中间件。

**当前边界（诚实口径）**：这是个人学习项目，全部在本地单机/容器环境验证。当前只有部分事务走了 `afterCommit`，点赞、关注方法并非都在事务中；消费者用容器 AUTO 确认，没有 publisher confirm、幂等判断、死信队列和 outbox，也没有 MySQL 与 Redis 的一致性补偿；取消点赞不会撤回已发通知，可能显示过时信息（生产上可给事件加版本或操作类型，消费者落库前查询当前点赞状态，或按"发送人 + 博客 + 类型"做可更新通知，是否撤回由产品口径决定）；Feed 的同分 offset 分支写反是已知 bug。

**生产环境如何升级**：第一，投递侧引入 Outbox，把"业务提交 + 事件记录"放进同一个事务，投递器/CDC 负责投递并更新状态；第二，消费侧补幂等：通知表加"接收人 + 类型 + 业务 ID"唯一约束或幂等表，配合 publisher confirm、手动 ACK、延迟重试与死信队列，把至少一次投递收敛为一次生效；第三，Feed 侧修复 offset 分支并补边界测试，大 V 改推拉结合，设置收件箱容量与过期策略，`SINTER` 等重操作移出请求链路。

## 小结

- 关注关系落 MySQL 为准，Redis Set 负责共同关注交集，Feed 用 ZSet 以时间戳为 score 排序。
- 纯推模式"写时扩散、读时一次查询"，适合普通用户；大 V 需要推拉结合与收件箱容量策略。
- 滚动分页用 `max + offset` 抵抗新 Feed 插入，同分边界要累加 offset；当前源码该分支写反，是待修复的已知 bug。
- 通知通过 durable direct exchange + durable queue + persistent 消息异步化，`afterCommit` 避免回滚误发，但提交后宕机仍会丢消息，需要 Outbox 补最终一致。
- RabbitMQ 是至少一次语义，消费幂等要靠唯一索引/幂等表，当前通知表只有普通索引，存在重复通知风险。
- 通知用 RabbitMQ、行为流用 Kafka，是不同吞吐与可靠性语义下的职责分工，不是重复堆技术。
