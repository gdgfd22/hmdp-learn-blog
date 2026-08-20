# 本地生活项目与相关高频 Java——面试精简回答

> 适用范围：`02-本地生活与实时运营平台-面试问题.md`、`03-比赛经历与基础知识-面试问题.md` 中与当前项目直接相关的内容，以及 `高频Java八股.md` 中能被项目自然追问到的高频题。
>
> 回答结构：先给结论，再讲项目实现，最后主动说明边界。主回答控制在 30—60 秒，追问控制在 15—30 秒。
>
> 整理方式：含义重复的问法合并回答；`02` 中直接相关的 P1/P2 技术点均已覆盖，但不机械复制重复题。
>
> 本稿不展开政务智能体、RAG/模型/视觉、泛网络/Linux，以及与当前项目没有自然追问关系的集合和 JVM 细节；这些继续保留在原题库中。

## 一、先统一口径

- 项目是个人学习项目。业务功能按 **C｜学习项目已实现** 表达，不说生产上线或生产集群。
- 实时分析链路已在单机 Docker Compose 环境完成实现与固定环境压测，核心 DWD 作业还做了故障恢复演练；完整 DWS 历史重放不在恢复实验范围内。可说“**C｜本地实现 + B｜固定环境实测**”，不能外推为生产容量。
- 二级缓存代码和定向单测已经存在，`CacheClientTest` 当前执行通过；但相关改动尚未提交，面试前应提交代码并保存测试记录。
- 当前商户查询主链路使用“缓存空值”的二级 Cache Aside；互斥重建和逻辑过期虽然有代码，但没有在 `queryById` 主链路启用。
- 秒杀使用 Lua + JVM 本地 `ArrayBlockingQueue`，RabbitMQ 只负责社交通知。两条链路不能混为一谈。
- 秒杀当前没有可靠队列、失败重试、Redis 补偿和订单状态查询闭环；异步入口也没有校验秒杀开始、结束时间。回答时必须主动说明。
- RabbitMQ 当前没有 publisher confirm/return、消费幂等、重试队列和死信队列；`afterCommit` 也不能消除事务提交后、消息发送前的宕机窗口。
- TraceID、MDC 和 AOP 当前覆盖单体 HTTP 请求，应说“请求级链路日志”，不说完整分布式追踪。
- 分析事件和 Doris 查询默认关闭，需显式开启配置；压测数据由脚本生成，不能说生产用户数据。
- 登录验证码当前只是生成后写 Redis 并输出 debug 日志，没有接入真实短信服务。

---

## 二、本地生活项目六个核心题

### Q1. 请用 60 秒介绍项目，并讲清业务系统和分析系统的关系

**面试精简版：**

> 这是我在点评课程项目上继续改造的个人学习项目。业务侧用 Spring Boot、MySQL 和 Redis 实现认证、商户缓存、关注 Feed、秒杀与社交通知；我重点新增了 JWT 双 Token、请求级 TraceID、Caffeine 二级缓存、RabbitMQ 通知和实时运营链路。行为事件写 Kafka，订单事实与券主数据由 CDC 采集，Flink SQL 做校验、去重和日聚合后写 Doris，供看板查询。业务系统产生事实，分析链路异步消费，不参与核心事务。项目只在单机容器和模拟数据下验证，不等同生产系统。

**追问 1：为什么给点评项目加入实时分析？**

> 点评业务不仅要完成交易，还需要运营侧及时看到商户热度、内容排行和优惠券漏斗。直接在 MySQL 上持续做多维聚合会干扰 OLTP，因此我把行为流和业务事实异步送入分析链路，实现负载隔离、事件回放和近实时指标计算。

**追问 2：业务系统和分析系统怎样连接？**

> 有两类契约。行为事件使用统一 JSON，核心字段包括 `event_id`、`event_type`、用户或设备标识、商户/博客/券/订单 ID、结果、事件时间、接入时间和扩展属性；订单事实与券主数据以 MySQL 表为源，由 CDC 读取变更。订单进入 DWD，券变更当前主要进入独立质量校验；不能把两者都说成已经完整参与主聚合。

**追问 3：哪些是课程代码，哪些是你的改造？**

> 课程基础主要是商户、博客、关注和秒杀等业务骨架。我的改造重点是 JWT access/refresh Token、请求级 TraceID 与 AOP 日志、Caffeine + Redis 二级缓存及多实例失效、RabbitMQ 社交通知、Kafka 行为埋点，以及 Kafka/Flink CDC/Flink SQL/Doris 的实时分析、看板、压测、恢复和对账脚本。

**追问 4：项目怎样启动？**

> MySQL、Redis、RabbitMQ、Kafka、Flink 和 Doris 由 `docker compose up` 启动；初始化完成后用 PowerShell 脚本提交 Flink SQL 作业；Spring Boot 应用单独通过 Maven 启动，并用环境变量开启埋点和 Doris 查询；前端由 Nginx 提供。当前不是把所有组件打成一个生产镜像的一键部署。

**追问 5：只展示一条链路，你展示什么？**

> 我会展示优惠券链路：请求先经过 Lua 做库存和一人一单校验，受理结果形成 Kafka 行为事件；订单由本地队列异步写入 MySQL，再由 CDC 进入 Flink；最终在 Doris 中分别计算请求数、受理数、实际订单数和支付数。它能同时说明业务高并发、异步一致性和指标口径，尤其能解释“受理成功不等于最终下单成功”。

---

### Q2. JWT、Redis、拦截器和 ThreadLocal 的完整认证链路是什么

**面试精简版：**

> 登录先校验 Redis 验证码，再签发 30 分钟的 HS256 accessToken；随机 refreshToken 对应的用户摘要以 Hash 存 Redis，TTL 7 天。普通请求只验 JWT，不查 Redis；拦截器把用户放进 ThreadLocal，登录拦截器再判断身份，请求结束统一 `remove()`。accessToken 过期后用 refreshToken 续签并延长会话。当前退出只删除 refreshToken，已签发的 accessToken 仍会有效到自身过期。

**追问 1：JWT 已经无状态，为什么还用 Redis？**

> Redis 不参与每次 accessToken 鉴权，主要保存 refreshToken 会话，使续期、退出和会话过期可控。这样普通请求保持无状态和低延迟，同时保留有限的服务端会话管理能力。

**追问 2：删除 refreshToken 后，accessToken 是否仍有效？**

> 当前仍然有效，直到 JWT 自身过期，因为普通鉴权不查 Redis。若要求立即封禁，可以缩短 accessToken TTL，并增加 token 版本、用户封禁状态或黑名单校验；安全要求更高时再接受每次鉴权的状态查询成本。

**追问 3：JWT 能放手机号、权限和敏感信息吗？**

> 不建议。JWT 默认只是 Base64URL 编码，不是加密，客户端可以读取载荷。当前只放用户 ID、昵称、头像和时间字段；敏感信息不进入 Token，权限也应尽量使用稳定标识并在服务端校验，避免权限变化后旧 Token 长期有效。

**追问 4：多请求同时刷新怎样避免重复刷新？**

> 当前 refreshToken 不轮换，并发请求可以同时签发多个 accessToken，这是学习版实现。生产方案会采用 refreshToken 轮换，用 Redis Lua 或原子删除加重建保证一次性使用，同时记录旧 Token 重用并撤销整个会话族。

**追问 5：异步线程和 MQ 消费者能直接拿到 ThreadLocal 吗？**

> 不能。ThreadLocal 绑定当前线程，线程池还会复用线程。业务身份应通过任务参数或消息体显式传递；TraceID 可以用任务装饰器复制 MDC，并在 `finally` 清理。MQ 消费者应以消息中的 userId、orderId 为准，不能依赖原请求线程。

**追问 6：为什么不用 Spring Security？**

> 这个个人项目为了聚焦认证原理，使用了自定义拦截器。若换成 Spring Security，我会把 JWT 解析放入 Security Filter Chain，把用户封装成 `Authentication` 写入 `SecurityContext`，用授权规则或方法注解替代登录拦截器，并统一处理 401、403、密码或验证码认证；Token 的签发和 Redis refresh 会话仍可复用。

**追问 7：Redis 宕机时哪些请求还能继续？**

> JWT 验签本身不依赖 Redis，因此只使用数据库且 accessToken 有效的请求理论上还能鉴权；登录、验证码、刷新以及商户缓存、Feed、秒杀等依赖 Redis 的功能会失败。当前没有完整的 Redis 熔断和数据库回源保护，不能说 Redis 故障时业务整体可用。

**追问 8：JWT 用什么算法，密钥怎样管理？**

> 当前使用 HMAC-SHA256，也就是对称密钥签名；验签方和签发方共享同一密钥。仓库里的 demo secret 直接写在 YAML 中，只适合本地学习。生产环境应从环境变量或密钥管理系统注入，限制读取权限，并用 `kid` 或密钥版本支持灰度轮换；轮换期间可短暂同时接受新旧密钥。

**追问 9：TraceID、MDC 和 AOP 的链路是什么？**

> order 0 的拦截器优先读取请求头 TraceID，没有就生成 UUID，把它写入 MDC 和响应头；Controller AOP 在正常和异常场景记录 URI、处理器、成功标志与耗时，异常继续抛出；请求完成后移除 MDC。当前没有 Span、跨服务采样，也没有把上下文传播到 Kafka、RabbitMQ 或线程池，所以准确口径是“请求级链路日志”，与 OpenTelemetry 的分布式追踪还有明显差距。

---

### Q3. Caffeine + Redis 二级缓存怎样读写和失效

**面试精简版：**

> 查询顺序是 Caffeine L1、Redis L2、MySQL，命中下层就向上回填；L1 最多 1 万条、30 秒过期，L2 正常数据 30 分钟，不存在数据用 2 秒空值防穿透。更新在数据库提交后清本机 L1、删 L2，再用 Pub/Sub 通知其他实例清 L1。它只保证最终一致：漏广播由 L1 短 TTL 兜底；L2 删除失败没有重试，旧值最坏可能持续到 30 分钟 TTL。
>
> 在多级缓存架构中，Caffeine 是每个服务实例独有的一级缓存，而 Redis 是多个实例共享的二级缓存。由于不同实例之间的 Caffeine 数据无法自动同步，所以更新数据后需要通过 Redis Pub/Sub 或 MQ 发布缓存失效消息，让其他实例删除本地缓存。更新流程一般采用 Cache Aside 模式，先提交数据库事务，确保数据库是最新数据，然后删除本地缓存和 Redis 缓存，最后通知其他实例清理 L1 缓存。这样即使存在短暂不一致，也可以通过 L1 短 TTL 或消息重试机制最终恢复一致。

什么是caffine

```
Caffeine 是一种高性能 JVM 本地缓存框架，通常用于缓存热点数据，作为 Redis 前面的一级缓存。相比 Redis，它直接访问本地内存，没有网络开销，性能更高。Caffeine 使用 Window TinyLFU 淘汰算法，并支持基于容量、时间的过期策略。在实际项目中一般采用多级缓存架构，先查询 Caffeine，不存在再查询 Redis，最后访问数据库。由于本地缓存存在多实例一致性问题，所以通常只缓存变化较少的数据，或者结合消息通知、版本号等机制进行缓存更新。
```

**追问 1：当前用了互斥重建和逻辑过期吗？**

> 两种方法都有实现，但当前 `queryById` 主链路只启用了空值缓存的 pass-through，它解决穿透，却没有解决热点 Key 同时失效后的并发回源。互斥锁和逻辑过期属于已经写出的替代方案，不能说三个策略同时在线生效；其中练习版互斥锁还是固定 value 加直接删除，也不能当成完整安全锁。

**追问 2：多实例 Caffeine 怎样保持一致？**

> 更新提交后发布失效 Key，各实例订阅 Redis Pub/Sub 并删除本地缓存。它延迟低、实现简单，但消息不持久；订阅者断线可能漏消息，所以当前还依赖 30 秒本地 TTL 兜底。生产环境可改为可靠 MQ、binlog 订阅或带版本号的缓存读取。

**追问 3：Pub/Sub、RabbitMQ、版本号和短 TTL 怎样选？**

> Pub/Sub 适合可容忍短暂不一致、希望低延迟广播的缓存；RabbitMQ 可确认和重试，更可靠但链路更复杂；版本号能在读取时拒绝旧版本，适合一致性要求更高的场景；短 TTL 最简单，但会增加 L2 或数据库压力。本项目选择 Pub/Sub 加短 TTL，定位是学习版最终一致。

**追问 4：先更新数据库再删缓存能绝对一致吗？**

> 不能。它降低了“先删缓存再更新数据库”产生长期旧值的概率，但仍有旧读回填和删除失败窗口。真正强一致要么不使用缓存，要么把读写串行化；多数高并发查询采用 Cache Aside，再用重试、binlog 或版本校验做最终一致。

**追问 5：逻辑过期为什么能返回旧值？**

> 缓存中保存数据和逻辑过期时间。未过期直接返回；已过期时只有抢到锁的线程异步重建，其他请求继续拿旧值，从而避免热点 Key 同时回源。它适合允许短暂陈旧但要求稳定延迟的商户详情，不适合库存、余额等强一致数据。

**追问 6：互斥锁没有抢到怎么办？**

> 当前代码休眠 50 毫秒后递归重试，能减少同一时刻的数据库回源，但缺少重试上限。生产实现应使用有界退避、超时和降级，必要时返回旧值；否则锁服务异常时会放大线程堆积。

**追问 7：穿透、击穿和雪崩分别怎样处理？**

```
Redis缓存问题主要包括缓存穿透、缓存击穿和缓存雪崩。缓存穿透是查询的数据在缓存和数据库中都不存在，导致请求持续访问数据库，常用方案是缓存空值，数据量较大时可以引入布隆过滤器。缓存击穿是热点Key失效瞬间大量请求访问数据库，可以通过互斥锁重建缓存或者逻辑过期解决。缓存雪崩是大量Key同时过期或者Redis故障导致流量冲击数据库，需要通过随机TTL、缓存预热、Redis高可用以及限流熔断保证系统稳定。
```

```
逻辑过期主要用于解决热点 Key 的缓存击穿问题。它不会给 Redis Key 设置真正的过期时间，而是在缓存 value 中保存一个逻辑过期时间。请求访问时判断是否过期，如果未过期直接返回；如果过期，则先返回旧数据，同时通过互斥锁让一个线程后台查询数据库并重建缓存，避免大量请求同时访问数据库。
```

> 穿透是数据在缓存和数据库都不存在，项目用短 TTL 空值缓存；击穿是单个热点 Key 失效，可用互斥重建或逻辑过期；雪崩是大量 Key 同时失效或 Redis 故障，要用随机 TTL、预热、高可用、限流和熔断。布隆过滤器只作为数据量更大时的升级方案，当前没有落地。

**追问 8：怎样发现热点 Key、大 Key 和缓存收益？**

> 热点和大 Key 可结合 Redis `hotkeys`、`bigkeys`、内存采样、慢日志和命令耗时分析。收益要同时看 Caffeine 命中率、Redis `keyspace_hits/misses`、数据库查询 QPS、接口 P95/P99 和资源占用。当前 Caffeine 开启了 `recordStats()`，但还没有导出监控，因此不能编造命中率或性能提升数字。

---

### Q4. 秒杀从请求进入到订单落库，完整链路是什么

**面试精简版：**

> 异步秒杀先生成订单 ID，用 Redis Lua 原子校验库存和一人一单并预扣；成功后通过 `offer` 写入本地有界队列。单线程消费者取得任务后加用户维度 Redisson 锁，再经 Spring 代理执行数据库事务：条件扣库存防超卖，唯一索引防重复。接口返回订单 ID 只表示受理。当前队列不持久，入队、抢锁或落库失败没有补偿、重试和状态查询，入口也未校验活动起止时间，所以只能叫本地削峰验证。
>
> 秒杀请求进入后，首先通过 Redis Lua 脚本完成库存预扣和一人一单校验，成功后生成订单消息并放入异步队列削峰。消费者线程消费订单，通过用户维度分布式锁保证同一用户串行创建订单，最后进入事务方法，在数据库层通过库存条件更新和唯一索引防止超卖和重复下单。当前实现是本地队列方案，缺少 MQ 持久化、失败重试和订单状态管理，只适合作为削峰验证方案。



**追问 1：Lua 为什么原子？脚本报错会自动回滚吗？**

> Redis 在执行脚本期间不会插入其他命令，所以“查库存、查资格、扣库存、记资格”不会被并发打断。但 Lua 的原子执行不等于数据库事务回滚；脚本运行到一半报错时，前面已经执行的写命令不会自动撤销。因此脚本应先完成全部校验，再执行写操作。当前脚本也是先校验后写，但库存 Key 缺失时 `tonumber(nil)` 会报错，初始化和异常处理仍需补强。

**追问 2：单线程消费者为什么还需要 Redisson？**

> 单实例、单消费者下这把锁不是必要条件；它主要为多实例、其他下单入口或重复任务提供跨 JVM 互斥。当前按 userId 加锁还会把同一用户购买不同券也串行化，粒度偏粗，生产中可改为 `userId + voucherId`，并继续保留数据库唯一索引作为最终兜底。

**追问 3：Redisson 的 watchdog、可重入和正确解锁是什么？**

> 项目调用未指定 leaseTime 的 `tryLock()`，成功后会启用 watchdog，默认给锁约 30 秒 TTL，并按租期约三分之一周期续期。Redisson 用客户端 ID 加线程 ID 标识持有者，用重入计数支持可重入；解锁必须在同一线程的 `finally` 中执行。watchdog 仍可能受长时间 GC、网络中断和主从切换影响，所以不能替代数据库幂等。

**追问 4：数据库条件更新锁住什么？**

> `voucher_id` 是秒杀券表主键，等值更新能够定位具体索引记录并加排他记录锁；`stock > 0` 作为条件使扣减成为单条原子更新，受影响行数为 0 就表示库存不足。如果条件不能命中索引，InnoDB 会扫描并锁住大量索引记录，效果接近锁全表，但不应简单说“直接升级成表锁”。

**追问 5：`stock > 0` 算乐观锁吗？**

> 它没有 version 字段，不是经典版本号乐观锁，但属于基于条件更新的乐观并发控制：先尝试原子修改，再根据受影响行数判断竞争是否成功。数据库不会把库存扣成负数。

**追问 6：扣库存成功、插订单失败会怎样？**

> 两个数据库操作在同一个 `@Transactional` 方法中，运行时异常会让 MySQL 库存回滚；项目通过注入自身 Spring 代理调用，避免同类直接调用绕过事务。但 Redis 的预扣和资格记录不在这个本地事务内，当前不会跟着回滚，这是尚未补齐的跨存储一致性问题。

**追问 7：本地队列满时 `add/offer/put` 有什么区别？**

> `add` 满时抛异常，`offer` 立即返回 `false`，`put` 会阻塞等待空间。项目使用 `offer`，不会长期占住请求线程；但 Lua 已预扣后入队失败没有补偿，用户会被占库存且无法重试。容量虽然有界，但一百多万条对象仍有明显内存风险，生产中必须按对象大小、峰值和消费能力重新定容量。

**追问 8：应用宕机或多实例部署会怎样？**

> 宕机会丢失该 JVM 队列中尚未落库的任务，Redis 资格和库存仍保留。多实例各有独立队列，虽然 Lua 是共享入口、数据库还有唯一索引，但某个节点宕机仍会丢它已接收的任务，也无法统一查看积压。消费者 `tryLock` 失败时当前同样直接丢弃任务，没有重试。当前只能叫“本地异步削峰验证”。

**追问 9：为什么升级到 Redis Stream 或 RabbitMQ，而不是 Kafka？**

> 异步下单是业务命令，更需要 ACK、未确认任务、重试、死信和补偿。Redis Stream 适合已有 Redis、规模较小的改造；RabbitMQ 的业务路由和失败处理更成熟。Kafka 当然也能承载，但它更擅长高吞吐事件日志和回放，本项目已把 Kafka 用在分析事件流上，职责更清晰。

**追问 10：当前为什么没用 RabbitMQ？如果改用，完整方案怎样设计？**

> 当前没用 RabbitMQ 不是因为它不适合，也不是为了少引入一个组件——项目的社交通知已经使用 RabbitMQ。真实原因是秒杀仍保留了学习阶段的本地队列实现，用来先验证“Lua 前置校验、请求快速返回、异步落库”主链路，尚未完成可靠消息化改造。本地队列没有持久化、ACK、重试、死信和统一积压监控，进程宕机或 `offer`、抢锁、落库失败都会留下 Redis 已预扣但订单未落库的问题，多实例之间也无法共享任务。
>
> 如果改用 RabbitMQ，入口先生成 orderId，Lua 原子完成库存、资格校验和预占；成功后 Java 构造带 `messageId、orderId、userId、voucherId` 的订单命令，持久化发送到 durable direct exchange 和订单队列，生产端开启 publisher confirm 与 mandatory/return。消费者设置合理的并发数和 prefetch，以手动 ACK 消费：在同一个 MySQL 事务里按 orderId 幂等校验、执行 `stock > 0` 条件扣减并插入订单，事务提交后才 ACK；若订单已存在则不重复扣库存，直接按成功结果确认。数据库订单主键和 `(user_id, voucher_id)` 唯一索引作最终兜底。临时异常进入有次数上限的延迟重试，毒消息或超过阈值的任务进入死信队列并告警；确认 MySQL 没有成功订单后，才用带 orderId 和状态 CAS 的幂等 Lua 恢复 Redis 库存、删除资格，同时提供 `PENDING/SUCCESS/FAILED` 状态查询。
>
> 还要主动说明：RabbitMQ 只解决任务持久化投递，不能自动保证“Redis Lua 预扣 + RabbitMQ 发送”原子完成。即使开启 confirm，仍存在 Lua 成功后应用宕机、消息尚未发送的窗口。生产方案需要保存可重试的预占记录并做定时扫描与 Redis/MySQL 对账；如果更强调入队原子性，也可以让 Lua 在同一脚本中 `XADD` 到 Redis Stream，再由消费者处理。若改为 MySQL 先记录订单意图，则可使用数据库 Outbox，但需要重新设计入口削峰和库存口径。

**追问 11：Redis Cluster 下 Lua 多 Key 有什么限制？**

> Redis Cluster 要求一个脚本访问的所有 Key 位于同一 hash slot，并且应通过 `KEYS` 明确传入。可以把库存和订单 Key 都写成带相同 hash tag 的形式，例如 `seckill:{voucherId}:stock` 和 `seckill:{voucherId}:order`。当前脚本动态拼 Key 且运行在单机 Redis，不能直接宣称兼容 Cluster。

---

### Q5. Redis Set/ZSet、Feed 和 RabbitMQ 通知怎样协作

**面试精简版：**

> 关注关系落 MySQL，并用 Redis Set 支持共同关注；点赞和 Feed 用 ZSet，score 是时间，Feed 当前采用纯推模式。设计上用 `max + offset` 滚动分页，但源码的同分 offset 分支写反，修复前可能重复或漏数据。点赞、关注和评论通知经 durable direct exchange 路由到 durable queue，消息默认 persistent，消费者写通知表。当前只有部分事务的 `afterCommit` 和容器 AUTO 确认，没有 confirm、幂等、死信或 outbox，也没有 MySQL 与 Redis 的一致性补偿。

**追问 1：Feed 是推、拉还是推拉结合？**

> 当前是纯推模式，适合粉丝规模不大的普通用户：发布成本增加，但读取快。大 V 会产生写放大，生产中应对普通用户继续推，对大 V 改为读时拉取或推拉结合，并设置收件箱容量和过期策略。

**追问 2：为什么滚动分页使用 `max + offset`？**

> 正确思路是：下一页把上一页最小 score 作为新的 max；若上一页边界仍是同一个 score，就把本次同分条数与旧 offset 累加，否则从本次同分条数重新计数。这样比页码分页更能抵抗新 Feed 插入，member 也用唯一博客 ID 去重。但当前代码把这个三元分支写反，会在全同分时重复、跨到新 score 时多跳；面试前应修复并补边界测试。

**追问 3：Set 交集有什么风险？**

> `SINTER` 适合中小关注集合，但大集合求交会占用 Redis 主线程并产生大结果。规模扩大后可限制集合规模和返回量、离线预计算，或改用数据库/图存储，不在请求链路无界返回。

**追问 4：事务提交后、消息发送前宕机会丢吗？**

> 会。`afterCommit` 只能避免事务回滚后误发；数据库已经提交、回调尚未发出时进程宕机，消息仍会丢。另外点赞和关注方法当前并非都处于事务中，有些通知会在数据库操作后直接发送。

**追问 5：Outbox 怎样解决数据库与消息的一致性？**

> 在同一个数据库事务中同时写业务数据和 outbox 事件；独立投递器或 CDC 读取未发送事件并投递 MQ，确认后更新发送状态。即使投递器宕机，事件记录仍可重试；消费者再用业务唯一键保证幂等，从而形成最终一致闭环。

**追问 6：消费成功但 ACK 前宕机会怎样？**

> Broker 会重新投递，因此至少一次语义会产生重复。项目通知表目前只有普通索引，没有基于“接收人 + 类型 + 业务 ID”的唯一约束，消费者也没有幂等判断，所以重复消息可能插入重复通知，这是当前缺口。

**追问 7：RabbitMQ 能保证 exactly-once 吗？**

> 不能把 Broker 投递等同业务 exactly-once。常见做法是生产确认、持久化、消费成功后 ACK，再接受可能重复的至少一次投递，并通过数据库唯一索引、幂等表或状态机把业务效果收敛为一次。

**追问 8：点赞后又取消，通知怎样避免过时？**

> 当前取消点赞不会撤回已发通知，因此可能显示过时信息。生产方案可以给事件增加版本或操作类型，消费者落库前查询当前点赞状态，或者按 `sender + blog + type` 做可更新通知；是否撤回还要由产品口径决定。

**追问 9：为什么通知用 RabbitMQ，行为流用 Kafka？**

> 通知是低到中吞吐的业务消息，重视路由、ACK、重试和死信；行为日志是高吞吐、可回放的事件流，要供多个 Flink 作业用不同消费组独立读取，更适合 Kafka。二者不是重复堆技术，而是服务不同语义。

---

### Q6. Kafka、Flink CDC、Flink SQL 与 Doris 的完整实时分析链路是什么

**面试精简版：**

> 浏览、点赞、关注、券曝光和秒杀请求以行为事件写 Kafka；订单事实和券主数据由 Flink CDC 读取，当前订单进入主 DWD，券变更主要做质量校验。Flink SQL 完成校验、按 eventId 去重、脏数据分流和自然日聚合，再写入 Doris Unique Key 表供看板查询。链路在单 Broker、Flink 并行度 2、单 FE/BE 环境验证；压测脚本直接写 Kafka，不包含 HTTP，故障恢复也只验证核心 DWD，不能外推为生产吞吐或全链路 exactly-once。

**追问 1：为什么行为直接写 Kafka，订单走 CDC？**

> 浏览和曝光天然是事件，量大且不一定需要进入业务库，直接写 Kafka 可以降低延迟和 MySQL 写压力。订单是核心交易事实，必须先由 MySQL 事务保证正确，再通过 CDC 分发状态变化；分析链路不能反过来成为订单事实源。

**追问 2：Flink CDC 的快照和 binlog 怎样衔接？**

> 当前使用 `initial` 模式。CDC 先读取存量快照，同时记录一致的 binlog 位点，快照完成后从对应位置继续消费增量；作业状态和位点随 Checkpoint 保存。恢复能力依赖可用的 Checkpoint Volume，删除状态后不能说从原位点恢复。

**追问 3：CDC 的 update_before/update_after 怎样处理？**

> CDC 在 Flink 中形成 Changelog。主键表的更新会以撤回旧值、加入新值或 upsert 的语义向下游传播；Upsert Kafka 以主键作为消息 Key，Doris Unique Key 保存最终版本；聚合算子则根据 Changelog 对旧贡献做撤回、对新贡献做累加。

**追问 4：Kafka 分区内有序和全局有序有什么区别？**

> Kafka 只保证同一 Partition 内的追加顺序，不保证跨分区全局顺序。应用 ODS 埋点按 userId 或 deviceId 选 Key，所以能获得用户维度的局部顺序；DWD Upsert 主题再按业务主键表达更新。全局只用一个分区会明显牺牲吞吐，通常没有必要。

**追问 5：行为怎样去重，状态保留多久？**

> DWD 以应用生成的 `event_id` 为去重 Key，用 `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts)` 保留事件时间最早的记录；若同一 ID 的两条记录连 `event_ts` 也相同，当前缺少稳定 tie-breaker。状态空闲 TTL 配为 2 天，它按处理时间清理长时间未访问的状态，并不等于“事件时间满两天”；状态被清理后再来的重复数据可能重新进入计算。Doris 明细表还以 `event_id` 为 Unique Key 收敛最终版本。

**追问 6：Event Time、Processing Time 和 Watermark 是什么？**

> Event Time 是事件真实发生时间，Processing Time 是 Flink 当前处理时间；Watermark 是算子对事件时间进度的估计。项目从 `event_time` 生成事件时间，Watermark 比当前观测到的最大事件时间滞后 10 秒；这不等于自动“容忍 10 秒迟到”，数据是否被丢弃或侧出还取决于具体窗口和算子。当前 DWS 主要按自然日持续聚合，没有使用事件时间滚动窗口。

**追问 7：迟到数据怎样处理？**

> 当前质量规则会把早于当前时间 7 天或晚于当前时间 5 分钟的事件判为异常；自然日聚合在状态 TTL 范围内仍可被迟到更新。项目没有实现通用的“超 Watermark 侧输出 + 自动回补”闭环，所以不能说所有迟到数据都已处理。生产中要按指标选择丢弃、侧输出、回补或修正历史结果。

**追问 8：Checkpoint、RocksDB 和 Doris 2PC 怎样协作？**

> 核心作业每 10 秒做一次 Exactly-Once Checkpoint，Kafka 消费位点和 Flink 状态一起保存，状态后端配置为 RocksDB；Doris Sink 开启 2PC，在 Checkpoint 成功后提交对应写入。Doris Unique Key还能把同一业务主键的重复写收敛为覆盖。但端到端语义仍取决于生产端、CDC、恢复方式和外部系统，所以我只说“降低重复和丢失风险”，不笼统承诺绝对 exactly-once。

**追问 9：为什么把 Checkpoint 从 30 秒改成 10 秒？**

> 本地压测发现 Doris 2PC 的可见性受 Checkpoint 提交周期影响。改为 10 秒后，DWD 可见 P95 上界从 36.05 秒降到 21.93 秒，最大 Kafka Lag 从 30,000 降到 9,991；代价是 Checkpoint 平均耗时从约 298 毫秒增到约 951 毫秒。这个 P95 含轮询和查询开销，不是看板 SLA。

**追问 10：Flink 反压怎样定位？**

> 从出现反压的算子沿数据流向下游查，比较各 Subtask 的 busy、backpressured、idle、吞吐和数据倾斜，同时看 Kafka Lag、Checkpoint 时长和 Doris Load/事务状态。少数 Subtask 忙通常先查热点 Key；所有上游都被压住则继续找最慢的 Sink 或状态算子。

**追问 11：Doris 为什么用 Unique Key？**

> CDC 订单和日聚合都会持续更新，同一订单 ID 或“日期 + 业务维度”会多次写入。Unique Key Merge-on-Write 能保留主键的最新版本，配合 2PC 降低恢复重放产生的重复。当前桶数和节点数只是单机演示配置，不能当生产分桶方案。

**追问 12：RabbitMQ 和 Kafka 能不能只留一个？**

> 技术上能勉强统一，但会损失各自优势。RabbitMQ 服务需要路由和业务确认的社交通知；Kafka 保存可回放的行为事件并支撑多个 Flink 消费组。当前秒杀本地队列也不应被描述为 RabbitMQ 或 Kafka。保持职责边界比为了“少一个组件”强行统一更清晰。

**追问 13：行为事件发送失败怎么办？**

> 当前 Kafka Producer 配置 `acks=all` 和 3 次重试，成功提交后由异步回调记录结果；但 `KafkaTemplate.send()` 获取元数据或等待缓冲空间时仍可能阻塞并同步抛运行时异常，而当前发布器没有捕获这类异常，所以 Kafka 故障可能拖慢或打断请求。回调阶段的失败只记日志，也没有 outbox、本地缓冲或补发。生产环境应先隔离发布异常，再用 outbox/CDC 或本地持久化队列补齐可靠投递。

---

## 三、数据分析专项详细版：基础概念与项目深挖

> 使用方式：先掌握 A 组基础概念，再按 Kafka → Flink CDC/Flink SQL → Doris/指标的顺序复习。回答时先说定义，再落到本项目，最后说明当前边界。

### A. 数据仓库与指标基础

#### 1. OLTP 和 OLAP 有什么区别

> OLTP 面向订单、库存等高并发事务，特点是单次读写数据量小、强调一致性和低延迟；MySQL 业务库属于 OLTP。OLAP 面向多维统计、趋势和排行，通常扫描更多数据、强调聚合吞吐；Doris 属于 OLAP。本项目把实时聚合查询从 MySQL 隔离到 Doris，避免看板 SQL 干扰交易。

#### 2. 批处理和流处理有什么区别

> 批处理先积累一批有界数据再统一计算，适合离线报表和历史重算；流处理持续接收无界数据并增量更新结果，适合实时看板。Flink 采用流批统一模型，但本项目作业运行在 streaming 模式，DWS 指标会随着事件和订单变更持续更新。

#### 3. ETL 和 ELT 有什么区别

> ETL 是先抽取、转换，再写目标库；ELT 是先把原始数据加载到目标平台，再在平台内转换。本项目更接近流式 ETL：Kafka/CDC 提供源数据，Flink 在写入 Doris 前完成校验、去重和聚合。若保留完整 ODS 后在 Doris 内重算，则会更接近 ELT。

#### 4. 事实表和维度表分别是什么

> 事实表记录可度量的业务过程，例如行为事件、订单、支付金额；维度表描述分析视角，例如商户、优惠券属性和用户标签。当前 `tb_voucher_order` 是订单事实源，`tb_voucher` 更接近券主数据；券 CDC 目前主要做质量校验，没有完整参与主 DWD 维度关联。

#### 5. 指标、维度和粒度分别是什么

> 指标是可计算的数值，如订单数、GMV、UV；维度是切分指标的角度，如日期、商户、优惠券；粒度说明一行数据代表什么。例如 `dws_order_day` 的粒度是“日期 + 优惠券 + 商户”，换粒度会改变唯一键、聚合 SQL 和指标含义，所以设计表前必须先定粒度。

#### 6. ODS、DWD、DWS、ADS 分别做什么

> ODS 尽量保留原始行为和 CDC 变更；DWD 做字段标准化、合法性校验、去重和脏数据分流；DWS 按主题和公共粒度聚合；ADS 面向具体看板组合指标。本项目 ODS 是 Kafka 行为 Topic 与 MySQL CDC，DWD 输出行为/订单明细，DWS 有平台、用户、商户、博客、券、订单日汇总，ADS 用 Doris View 生成总览、排行、漏斗和留存。

#### 7. 为什么不直接从 ODS 一条 SQL 算到 ADS

> 技术上可以，但清洗、公共口径和页面逻辑会耦合，多个看板还会重复计算。分层后 DWD 可以复用，DWS 统一公共指标，ADS 只做轻量组合；出现异常时也能沿 ODS → DWD → DWS → ADS 逐层定位。

#### 8. PV、UV 和 DAU 怎样区分

> PV 是事件次数，同一用户访问两次计两次；UV 是在指定维度和时间范围内去重后的用户数；DAU 是当天产生有效行为的独立用户数。本项目登录用户使用 `u:userId`，匿名设备使用 `d:deviceId` 形成 `user_key`，避免数字 ID 和设备 ID 冲突，但没有完成匿名设备登录后的身份合并。当前 DAU 只来自合法行为流，只有订单 CDC、没有行为事件的用户不会进入 DAU。

#### 9. GMV、退款金额和净 GMV 怎样定义

> 当前 GMV 是存在 `pay_time` 的订单 `pay_amount` 之和，金额单位统一为分，避免浮点误差；退款金额单独汇总 `refund_amount`。项目没有直接用退款冲减 GMV，如果业务要看净交易额，应另定义 `net_gmv = gmv - refund_amount`，不能把三种口径混用。

#### 10. 漏斗和留存分别解决什么问题

> 漏斗衡量同一业务路径各阶段的转化，例如曝光 → 请求 → 订单 → 支付；留存衡量同一批用户经过若干天是否再次活跃。当前漏斗按“日期 + 券 + 商户”关联行为和订单，次日/7 日留存先找用户首次活跃日，再检查第 1 天、第 7 天是否回访。

#### 11. 精确去重和近似去重怎样选择

> `COUNT(DISTINCT user_key)` 结果直观，但要维护较大的状态，用户基数越大，RocksDB 和 Checkpoint 成本越高。当前项目为演示正确口径使用精确去重；规模扩大后可用 HyperLogLog、分桶去重或两阶段聚合，以可控误差换更低状态成本。

#### 12. 至少一次、至多一次和恰好一次是什么

> 至多一次可能丢但不重复；至少一次尽量不丢但可能重复；恰好一次要求每条数据对最终业务结果只生效一次。工程上通常通过可恢复 Source、状态快照、事务或幂等 Sink 分段实现。本项目配置了 Flink Exactly-Once Checkpoint 和 Doris 2PC，但应用埋点、Kafka、CDC、Flink、Doris 必须分段分析，不能一句话承诺全链路绝对 Exactly-Once。

### B. Kafka 基础与项目细节

#### 13. Topic、Partition、Replica 分别是什么

> Topic 是消息的逻辑分类；Partition 是并行读写和局部顺序的基本单位；Replica 是分区副本，用于故障冗余。当前本地 Topic 使用 3 个分区，但 Kafka 只有单 Broker、副本数为 1，因此只能验证功能和吞吐，不能抵御节点级数据丢失。

#### 14. Consumer Group、Offset 和 Lag 分别是什么

> Consumer Group 是协作消费同一 Topic 的一组消费者，同组内一个 Partition 同时只交给一个消费者；Offset 表示分区中的消息位置；Lag 是最新 Offset 与已消费或已提交位置的差。Lag 持续上涨说明消费赶不上生产或位点无法稳定推进，但还要结合反压、Checkpoint、分区倾斜和 Sink 状态定位。

#### 15. 本项目 Kafka Key 为什么按用户或设备选择

> `KafkaBehaviorEventPublisher` 优先使用 userId，未登录时使用 deviceId，都没有时退化为 eventId。这样同一用户或设备的事件通常进入同一 Partition，获得用户维度局部有序；但热点用户也可能形成倾斜。DWD Upsert Topic 则必须使用事件 ID 或订单 ID 这类业务主键表达更新语义。

#### 16. Kafka 能保证全局有序吗

> Kafka 只保证单 Partition 内的追加顺序，不保证跨 Partition 全局有序。若把 Topic 设为一个分区可以获得全局顺序，但会牺牲吞吐和并行度。项目只需要用户或业务主键维度的局部顺序，不追求全局顺序。

#### 17. Kafka 分区数怎样确定

> 要结合目标吞吐、单分区能力、消费并行度、Key 倾斜和未来扩容余量压测决定。Source 并行度高于分区数时，多出的并行实例通常没有数据；分区很多也会增加元数据、文件句柄和 Rebalance 成本。当前 3 分区、Flink 并行度 2 只是单机实验值，不是生产容量规划。

#### 18. ISR、`acks=all` 和 `min.insync.replicas` 怎样协作

> ISR 是与 Leader 保持同步的副本集合；`acks=all` 要求满足条件的同步副本确认，`min.insync.replicas` 决定至少需要多少 ISR 才允许写入。它们能降低 Broker 故障丢消息的风险，但当前单 Broker、副本 1 环境没有真正的副本容灾。

#### 19. Kafka 幂等生产者和事务解决什么

> 幂等生产者通过 Producer ID 和序列号避免网络重试在同一 Partition 产生重复；Kafka 事务可把一组 Kafka 写入以及“消费 Kafka 再写 Kafka”的位点提交原子化。它们不能自动把 MySQL 事务包含进来，跨 MySQL 和 Kafka 仍需 Outbox、CDC 或业务补偿。

#### 20. 为什么不同 Flink 作业要使用不同 Consumer Group

> DWD 清洗、DWS 聚合和质量检测的消费目的不同，每个任务都需要完整读取自己的输入。如果共用一个 Group，分区会在任务之间分配，导致每个任务只读到部分数据。不同 Group 各自维护 Offset，也便于独立暂停、恢复和回放。

#### 21. `group-offsets` 和 `earliest` 是什么关系

> DWD 普通 Kafka Source 使用 `scan.startup.mode=group-offsets`，优先从该 Group 已提交位置继续；当没有历史位点时，`auto.offset.reset=earliest` 才作为回退。它避免每次重提作业都固定从最早数据重新消费。若更换 Group，则等同一个新的消费进度，需要明确是否允许重放。

#### 22. Rebalance 为什么会发生，有什么影响

> 消费者加入、退出、故障，订阅 Topic 的分区变化或协调超时都可能触发 Rebalance。期间消费会短暂停顿，若处理结果与 Offset 提交边界没有对齐，还可能重复处理。Flink 依赖 Checkpoint 管理 Source 位点，下游仍需幂等，不能把 Rebalance 当成纯性能问题。

#### 23. Kafka Lag 上涨怎样逐层排查

> 先看作业和消费者是否存活，再看各分区 Lag 是否均匀；然后检查 Flink Source 输入、算子反压、Checkpoint 是否持续失败以及 Doris Sink 是否变慢。只有少数分区高 Lag 通常先查 Key 倾斜；所有分区一起上涨更可能是整体消费能力或下游瓶颈。

#### 24. 应用发送 Kafka 的完整过程和当前风险是什么

> 应用把行为对象序列化为 JSON，设置 Topic 和 Key 后调用 `KafkaTemplate.send()`；正常情况下先进入 Producer Buffer，再由 Sender 线程批量发送，结果通过回调返回。获取元数据或等待缓冲空间仍可能同步阻塞并抛异常，而当前发布器只捕获 JSON 序列化异常，因此 Kafka 故障可能拖慢或打断请求；回调失败也只有日志，没有 Outbox 和补发。埋点功能默认关闭，未开启时注入 Noop Publisher，不会真正发送 Kafka。

#### 25. Kafka 保留日志为什么有利于回放

> Kafka 消费后不会立即删除消息，而是按保留时间或大小清理，因此新 Consumer Group 可以从指定 Offset 重放。回放时必须沿用 eventId、保证下游幂等，并明确状态 TTL 是否仍覆盖这段历史。超过 Kafka 保留期的数据不能再依赖 Kafka，需要长期 ODS 或离线归档。

#### 26. JSON 事件怎样做 Schema 演进

> 当前 JSON 解析允许缺少字段并忽略解析错误，但没有 Schema Registry。兼容演进应优先新增可选字段、给默认值、保留旧字段一段时间；删除、改名和类型变化要版本化并灰度升级生产者与消费者。生产环境可引入 Avro/Protobuf + Schema Registry，明确兼容策略。

### C. Flink CDC、Flink SQL 与状态基础

#### 27. Flink CDC 的基本原理是什么

> MySQL CDC 首次可读取存量快照，之后持续读取 row 格式 binlog，将 INSERT、UPDATE、DELETE 转换为 Flink Changelog。订单属于核心事实，先由 MySQL 事务保证正确，再由 CDC 分发。CDC 不是定时全表扫描，也不能替代业务库事务。

#### 28. `initial` 快照怎样与 binlog 增量衔接

> 作业读取快照时会记录一致的 binlog 位点，快照完成后从对应位置继续处理增量，避免快照期间的更新被遗漏。Source 位点随 Checkpoint 保存；恢复必须依赖仍存在的 Checkpoint/Savepoint 和 binlog，删除状态 Volume 后不能说从原位置恢复。

#### 29. CDC 为什么要配置独立 `server-id`

> MySQL 把 CDC Reader 看作复制客户端，同一 MySQL 实例上的复制客户端 server-id 不能冲突。项目给订单和优惠券 CDC 配置了不同 server-id 范围，也为并行 Reader 预留多个 ID；生产中还要避免与真实主从复制、其他 CDC 作业冲突。

#### 30. `PRIMARY KEY NOT ENFORCED` 是什么意思

> 它告诉 Flink Planner 该字段在数据语义上是主键，但 Flink 不负责检查唯一性，正确性仍由上游保证。主键信息使 Changelog、Upsert Kafka 和 Doris Sink 能识别同一业务记录的更新。若上游主键实际重复，Planner 不会像 MySQL 唯一索引一样替你拦住。

#### 31. INSERT、UPDATE_BEFORE、UPDATE_AFTER、DELETE 怎样形成 Changelog

> 动态表更新不是只有追加。订单状态从未支付变为已支付时，聚合需要撤回旧状态贡献，再增加新状态贡献；删除则撤回整行影响。Upsert Kafka 通常以主键和最新值表达插入/更新，以 tombstone 表达删除；Doris Unique Key 保存最新版本。

#### 32. 行为事件的统一结构是什么

> 核心字段包括 `event_id`、`event_type`、userId/deviceId、shop/blog/voucher/order ID、result、eventTime、ingestTime 和 properties。eventId 默认 UUID，用于去重；eventTime 表示业务发生时间，ingestTime 是发布器接收并准备发送的时间；properties 承载 reason、followUserId 等扩展字段。但当前 DWD 只显式提取 reason，followUserId 没有进入 DWD，因此只能统计关注/取消关注趋势，不能分析关注目标关系；事件也没有独立 schemaVersion，这是后续契约治理缺口。

#### 33. 为什么构造统一的 `user_key`

> 登录用户构造 `u:userId`，匿名设备构造 `d:deviceId`，这样 DAU/UV 可以统一对一个字段去重，并避免用户数字 ID 与设备字符串偶然冲突。当前没有登录前后身份合并，因此同一自然人在匿名和登录状态下可能被计成两个用户，这是指标边界。

#### 34. DWD 对行为事件做了哪些质量校验

> 校验 eventId、事件类型、用户或设备标识、事件时间，以及商户/博客/券等必需业务 ID；早于当前 7 天或晚于当前 5 分钟也会判异常。合法事件进入 DWD，非法事件写 dirty Topic 并带 error_reason。规则是演示口径，生产中应版本化、告警并支持修复回放。

#### 35. 订单质量校验包括什么

> 订单要求用户、券和商户 ID 非空，状态在 1—6，金额非负，实付加优惠不能超过原价快照，退款不能超过实付；已支付、已核销或退款状态必须有支付时间，退款状态还必须有退款时间。合法订单进入主 DWD，异常订单由独立质量任务统计。

#### 36. 行为事件怎样去重

> 当前按 eventId 分区，用 `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts)` 保留事件时间最早的记录，Doris 再以 eventId 作 Unique Key。这个方案需要状态，DWD 状态空闲 TTL 为 2 天；状态清理后再来的超晚重复可能重新进入。相同 eventId、相同 eventTs 还缺稳定 tie-breaker，生产中可增加 ingestTime 或来源序号。

#### 37. Event Time、Ingest Time、Processing Time 分别有什么用途

> Event Time 决定业务事件归属日期；Ingest Time 可观察应用侧采集延迟；Processing Time 是 Flink 当前处理时间，受负载和恢复影响。当前 Doris 明细的 `latency_ms` 实际计算为 `ingest_time - event_time`，它只反映事件发生到应用接入的时间差，不是 Kafka → Flink → Doris 的端到端可见延迟。

#### 38. Watermark 到底做什么

> Watermark 是算子对事件时间进度的估计，用于事件时间窗口触发和迟到判断。项目定义 `event_ts - 10 秒`，表示 Watermark 比已观察到的最大事件时间落后约 10 秒；它不等于所有晚 10 秒以内的数据一定正确、超过 10 秒一定丢弃。当前主 DWS 是自然日持续分组聚合，并没有靠 Watermark 关闭日窗口。

#### 39. Tumbling、Sliding、Session Window 怎样选择

> Tumbling Window 不重叠，适合每分钟请求量；Sliding Window 会重叠，适合最近 10 分钟每分钟更新的趋势；Session Window 根据空闲间隔切分访问会话。当前核心指标是自然日累计，没有使用这些窗口；若新增分钟趋势，需要再定义 Watermark、允许迟到和历史修正。

#### 40. 为什么当前按自然日持续聚合

> 运营看板的核心口径是当天累计，所以直接把 eventTs 或 createTime 格式化为日期，再按日期和业务维度持续更新。这样迟到事件在状态仍保留时可以修改当天结果，但日期状态不会像有限窗口那样自动关闭，因此要依赖 TTL、重放策略和 Doris Unique Key 控制状态与更新。

#### 41. 为什么使用 Flink SQL，而不是全部写 DataStream API

> 当前任务主要是过滤、去重、分组聚合和多 Sink 写出，SQL 表达更直观，也便于查看执行计划和统一 Changelog 语义。复杂自定义状态、精细定时器、异步 I/O、复杂 CEP 或 SQL 难以表达的规则，才更适合 DataStream API。选型依据是问题表达，不是 SQL 一定比代码快。

#### 42. Flink 的 Operator、Task、Subtask、Slot、Parallelism 是什么

> Operator 是 map、聚合、Sink 等逻辑算子；相邻可链化算子会组成 Task；Task 按并行度拆成多个 Subtask；Task Slot 是 TaskManager 的资源份额；Parallelism 决定同一算子并行实例数。当前默认并行度 2，只是单 TaskManager 演示配置，不代表两个线程或两台机器。

#### 43. Flink 状态有哪些，为什么会膨胀

> 状态可分 Keyed State 和 Operator State；SQL 的去重、Distinct、聚合和 Join 会由 Planner 生成相应状态。Key 数不断增加、精确 UV 基数大、TTL 过长或 Join 无时间边界都会让状态膨胀，进而增加 RocksDB I/O 和 Checkpoint 大小。

#### 44. 状态 TTL 为什么 DWD 是 2 天、DWS 是 8 天

> DWD 主要限制 eventId 去重状态，2 天是重复重放窗口与成本的演示取舍；DWS 包含用户活跃和 7 日留存相关状态，所以配置 8 天。Flink Table TTL 是按处理时间的状态空闲清理，不是严格事件时间窗口；生产取值应结合最大迟到、回放周期和指标口径压测决定。

#### 45. 为什么使用 RocksDB 状态后端

> RocksDB 把状态放在本地磁盘与内存组合中，容量通常比纯 JVM 堆状态更大，适合精确 UV、去重和持续聚合；代价是序列化、磁盘访问和恢复开销。当前配置主要用于学习有状态流处理和恢复，单机测试不能证明生产状态规模。

#### 46. Checkpoint 的基本过程是什么

> JobManager 触发 Checkpoint Barrier，Barrier 随数据流经过各算子；算子在一致位置快照状态，Source 位点和 Sink 事务也参与同一次 Checkpoint。全部确认后 Checkpoint 才成功。项目间隔 10 秒、超时 5 分钟、最小暂停 3 秒，同时只允许一个 Checkpoint。

#### 47. Barrier 对齐和 Unaligned Checkpoint 是什么

> 多输入算子做对齐 Checkpoint 时，先到的 Barrier 会等待其他输入，反压严重时对齐时间会拉长。Unaligned Checkpoint 会把在途数据一起纳入快照，能降低严重反压下的对齐等待，但会增大快照并提高恢复成本。当前项目没有把 Unaligned Checkpoint 作为已落地优化。

#### 48. Checkpoint 和 Savepoint 有什么区别

> Checkpoint 主要由系统自动触发，用于故障恢复；Savepoint 通常由用户主动触发，用于升级、迁移和计划内停机。两者都保存状态，但生命周期和运维目的不同。当前完成的是核心 DWD Checkpoint 恢复实验，没有完整验证 DWS 历史重放和 Savepoint 版本升级。

#### 49. TaskManager 宕机后的恢复链路是什么

> TaskManager 恢复后，作业从最近可用 Checkpoint 恢复算子状态和 Kafka 位点，再继续消费；随后检查新 Checkpoint、Kafka Lag 回落和 Doris 主键结果。固定实验中只验证核心 DWD：约 23.8 秒恢复 RUNNING、37.8 秒产生新成功 Checkpoint、50.9 秒 Lag 回到 0，2 万条事件写入 DWD 明细。不能说完整 DWS 全链路同样恢复。

#### 50. Flink 的 Exactly-Once 要分哪几段回答

> Flink 内部状态靠 Exactly-Once Checkpoint；Kafka Source 位点随状态恢复；Doris Sink 使用 2PC，并以 Unique Key 收敛重复写。应用 Producer 仍可能失败，Kafka 当前无副本高可用，CDC、状态 Volume 和恢复操作也有边界。因此正确说法是“核心 Flink 状态和 Doris 提交做了 Exactly-Once 配置”，不是“全系统任何故障都绝对不丢不重”。

#### 51. 什么是反压，怎样定位

> 下游处理速度低于上游输入时，缓冲区逐渐占满，压力向上游传播。先在 Flink UI 比较各算子的 busy、backpressured、idle、输入输出速率和 Subtask 差异，再沿数据流向下游找最慢点；同时看 Kafka Lag、Checkpoint 对齐/耗时和 Doris 导入事务。不要只看到 Source 反压就盲目增加 Source 并行度。

#### 52. 数据倾斜怎样发现和处理

> 少数 Subtask 的 Records、Busy、Backpressure 明显高于其他实例，通常说明热点 Key 或分区不均。可先确认 Kafka 分区分布，再考虑热点 Key 加盐后两阶段聚合、拆分大商户、调整分区键或单独处理热点。单个 Key 的数据最终仍要落到一个逻辑分组，盲目加并行度不一定有效。

#### 53. Upsert Kafka 为什么适合 DWD

> 订单 CDC 和动态表聚合都可能更新同一主键，不是纯追加流。Upsert Kafka 把主键序列化为 Kafka Key，新值表示插入或更新，null value 可表达删除，使下游看到最新表状态。`PRIMARY KEY NOT ENFORCED` 为 Planner 提供主键信息，但不检查唯一性。

#### 54. `EXECUTE STATEMENT SET` 有什么作用

> 它把多个 INSERT 作为一个 Flink 作业提交，可共享上游 Source 和公共计算，减少重复读取与部署数量。它不代表 Kafka、Doris 多张表之间拥有一个跨系统数据库事务；各 Sink 的提交和故障语义仍要分别分析。

### D. Doris、指标口径、质量与压测

#### 55. Doris 的 FE、BE 和 Tablet 分别是什么

> FE 负责元数据、SQL 解析、优化和调度；BE 负责数据存储和执行；Tablet 是表分区分桶后的数据分片。当前只有单 FE、单 BE、replication_num=1，因此没有节点级高可用，桶数 1 或 3 也只是演示配置。

#### 56. 列式存储为什么适合分析查询

> 分析查询往往只读取少数列并扫描大量行，列式存储能减少无关数据 I/O，并获得更好的压缩和向量化执行效果。MySQL InnoDB 更适合按主键或索引读取整行并执行事务；因此本项目让 MySQL 保证业务事实，让 Doris 承担聚合查询。

#### 57. Duplicate、Unique、Aggregate Key 怎样选择

> Duplicate Key 保留所有明细；Unique Key 按业务主键保留最新版本；Aggregate Key 在写入时按 Key 聚合 Value。当前行为、订单和 DWS 表都要持续更新同一主键，所以使用 Unique Key；如果要保存不可变原始日志，可考虑 Duplicate Key；固定预聚合模型才考虑 Aggregate Key。

#### 58. Merge-on-Write 和 Merge-on-Read 有什么区别

> MOW 在写入阶段完成同主键版本合并，查询直接读取最新结果，读性能更稳定；MOR 保留多个版本，在查询时合并，写入更轻但查询成本更高。当前表开启 Unique Key Merge-on-Write，符合看板频繁读取最新状态的需求。

#### 59. Doris 分区和分桶分别解决什么

> 分区通常按日期等粗粒度拆数据，支持查询裁剪和生命周期管理；分桶在分区内按 Hash/Random 再切 Tablet，影响并行度和数据分布。当前表没有日期分区，仅按事件 ID、订单 ID 或业务维度 Hash 分 1/3 桶；生产中要按时间保留、节点数、数据量和 Tablet 大小重新设计。

#### 60. Doris Sink 2PC 怎样与 Checkpoint 配合

> Sink 在 Checkpoint 周期内先预提交数据，Checkpoint 成功后再提交对应 Doris 事务；失败时该批不作为成功结果提交。它把外部写入与 Flink 状态快照对齐，但仍依赖稳定 Label、可恢复状态和 Doris 可用性。Unique Key 是幂等兜底，不等于替代 2PC。

#### 61. 本项目 DWS 有哪些表，粒度是什么

> `dws_platform_day` 是日期粒度；`dws_user_active_day` 是日期 + userKey；`dws_shop_day` 是日期 + 商户；`dws_blog_day` 是日期 + 博客；`dws_voucher_behavior_day` 是日期 + 券；`dws_order_day` 是日期 + 券 + 商户。每张表的 Unique Key 就对应其业务粒度。

#### 62. 商户热度和博客热度怎样计算

> 商户热度是 `访问UV + 净点赞数×3 + 订单数×5`，博客热度是 `浏览UV + 净点赞数×3`，净点赞为点赞数减取消点赞数。权重只是当前演示规则，没有经过业务实验、刷量治理或版本管理，面试时应说明“可解释，但不是行业固定公式”。

#### 63. 优惠券漏斗怎样计算

> 按日期、voucherId、shopId 连接行为汇总和订单汇总：请求率 = 秒杀请求数 / 曝光数，下单率 = 实际订单数 / 请求数，支付率 = 已支付订单数 / 实际订单数；分母为 0 时返回 0。`ACCEPTED` 只表示 Lua 通过且本地队列受理，不等于实际订单，所以看板同时展示 acceptedCount 和 orderCount。当前 rejectedCount 只统计 `REJECTED`，队列满发布的 `ERROR` 不属于 accepted/rejected，因此请求数不一定等于二者之和。

#### 64. 订单、支付和 GMV 当前归属哪一天

> `dws_order_day` 按订单 `create_time` 的自然日分组，paidOrderCount 和 GMV 判断 `pay_time` 是否非空。因此订单第二天支付后，会更新订单创建日对应的汇总，而不是计入支付发生日；退款订单只要存在 payTime 仍计入 paidOrderCount 和 GMV，退款金额另列。若运营需要“支付日 GMV”或净 GMV，应按 payTime/refundTime 另建主题指标，不能沿用当前口径。

#### 65. 次日和 7 日留存怎样计算

> 先从 `dws_user_active_day` 找每个 userKey 的首次活跃日期作为 cohort，再左连接首次日期后第 1 天和第 7 天的活跃记录。留存率是回访人数 / cohort 人数。当前身份未做匿名到登录合并，且 DWS 状态 TTL 与历史数据保留会影响长期口径。

#### 66. 项目做了哪些数据质量检查

> 行为侧检查事件 ID、类型、用户标识、业务 ID 和时间；订单侧检查状态、金额、支付与退款时间；券侧检查商户、券类型、状态和金额关系；重复事件还按分钟统计数量和样例。脏数据不会直接冒充正常指标，而是进入 dirty Topic 或 `ads_data_quality`。

#### 67. 脏数据为什么要保存样例和错误原因

> 只统计错误数无法定位生产者、字段和规则问题。保留原消息标识、错误原因、Schema/作业版本和少量样例，才能告警、修正规则并定向回放。生产中还要控制敏感信息和样例保存周期，不能把完整用户数据无限期留在质量表。

#### 68. MySQL 与 Doris 怎样对账

> 先固定日期和指标口径，再比较 MySQL 订单数、已支付订单数、GMV 与 Doris DWS 汇总；有差异时继续检查 CDC 位点、Kafka Lag、Checkpoint、非法订单质量记录和时区。当前保存的样本为 40 笔订单、30 笔已支付、GMV 142500 分，差异为 0，只证明该样本口径一致。

#### 69. 看板查询链路是什么

> Spring Boot 通过独立 Doris JdbcTemplate 查询 `ads_realtime_overview`、商户/博客排行、券漏斗和质量表，趋势接口查 `dws_shop_day`。前端不直接连接 Doris，便于参数校验、权限、口径封装、缓存和审计。当前查询默认关闭，需显式开启分析查询配置。

#### 70. 当前压测测了什么，没有测什么

> 三轮共 9 万条模拟行为由脚本直接写 Kafka，平均实际输入约 986 events/s，验证到 Doris DWD 明细可见；不包含 Spring Boot HTTP、业务数据库写入或完整 ADS 看板。10 秒 Checkpoint 下 DWD 可见 P95 上界约 21.93 秒，指标包含轮询和查询开销，不能当成业务接口 P95。

#### 71. 为什么 10 秒 Checkpoint 比 30 秒可见更快

> Doris 2PC 的正式提交与 Checkpoint 成功对齐，Checkpoint 间隔越长，数据等待提交的时间通常越长。改为 10 秒后，DWD 可见 P95 上界由 36.05 秒降到 21.93 秒、最大 Lag 由 30000 降到 9991；代价是平均 Checkpoint 耗时从约 298ms 增到约 951ms。间隔不能无限缩短，否则快照和事务开销会反过来压低吞吐。

#### 72. 数据分析链路最值得主动讲的难点是什么

> 第一是把“秒杀受理”和“最终订单成功”分开建模：入口行为只说明 Lua 通过，最终结果以 MySQL CDC 订单为准。第二是处理更新流：订单状态变化会产生 Changelog，DWS 需要撤回旧贡献并更新结果，因此使用 Upsert Kafka、Doris Unique Key、Checkpoint 和 2PC，而不是把所有数据当追加日志。

#### 73. 如果继续生产化，优先补什么

> 先补多 Broker、Flink HA、Doris 多副本和统一监控告警；再补事件 Schema Registry、Outbox/本地持久缓冲、分钟窗口与迟到回补；随后针对高基数 UV、热点 Key、DWS 全量回放和 Savepoint 升级做压测。指标还要版本化，并为隐私、权限和保留周期建立治理规则。

### E. 现场 SQL 基础

#### 74. 每日商户 PV、UV

```sql
SELECT
    CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE) AS metric_date,
    shop_id,
    SUM(CASE WHEN event_type = 'SHOP_VIEW' THEN 1 ELSE 0 END) AS visit_pv,
    COUNT(DISTINCT CASE
        WHEN event_type = 'SHOP_VIEW' THEN user_key
        ELSE NULL
    END) AS visit_uv
FROM dwd_behavior
WHERE shop_id IS NOT NULL
GROUP BY
    CAST(DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS DATE),
    shop_id;
```

#### 75. 按 eventId 去重

```sql
SELECT *
FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_ts ASC
        ) AS row_num
    FROM normalized_behavior t
)
WHERE row_num = 1;
```

> 追问时主动补充：若 eventTs 相同，应再增加 ingestTs 或来源序号作为稳定排序字段；流式去重还要设置状态 TTL。

#### 76. 计算优惠券三段转化率

```sql
SELECT
    CASE WHEN exposure_count = 0 THEN 0
         ELSE request_count * 100.0 / exposure_count END AS request_rate,
    CASE WHEN request_count = 0 THEN 0
         ELSE order_count * 100.0 / request_count END AS order_rate,
    CASE WHEN order_count = 0 THEN 0
         ELSE paid_order_count * 100.0 / order_count END AS pay_rate
FROM voucher_funnel_source;
```

#### 77. 计算次日留存

```sql
WITH first_active AS (
    SELECT user_key, MIN(metric_date) AS cohort_date
    FROM dws_user_active_day
    GROUP BY user_key
)
SELECT
    f.cohort_date,
    COUNT(*) AS cohort_size,
    COUNT(d1.user_key) AS d1_retained_users,
    COUNT(d1.user_key) * 100.0 / COUNT(*) AS d1_retention_rate
FROM first_active f
LEFT JOIN dws_user_active_day d1
  ON f.user_key = d1.user_key
 AND d1.metric_date = DATE_ADD(f.cohort_date, INTERVAL 1 DAY)
GROUP BY f.cohort_date;
```

### F. 数据分析复习顺序

1. 先在 30—60 秒内讲清“行为 Kafka + 订单 CDC → DWD → DWS → Doris ADS → Spring Boot 看板”。
2. 再掌握 ODS/DWD/DWS/ADS、事实/维度、指标/粒度、PV/UV/DAU/GMV/漏斗。
3. Kafka 优先背 Partition、Key、Consumer Group、Offset、Lag、acks/ISR 和 Rebalance。
4. Flink 优先背 CDC 快照衔接、Changelog、去重状态、Event Time/Watermark、Checkpoint 和反压。
5. Doris 优先背 Unique Key MOW、分区/分桶、2PC、DWS 粒度和 ADS View。
6. 最后手写 PV/UV、eventId 去重、漏斗率和留存 SQL。
7. 每个回答都以“当前实现 → 为什么这样做 → 当前缺口 → 生产升级”收尾。

---

## 四、核心题之外的 40 个直接相关追问

### 1. accessToken 和 refreshToken 的 TTL 如何选择

> TTL 是安全窗口和用户体验的取舍。当前 accessToken 为 30 分钟、refreshToken 为 7 天，普通鉴权不查 Redis；生产环境可按设备可信度、操作风险和客户端类型分级设置，并配合 refreshToken 轮换与会话族撤销，而不是只延长 TTL。

### 2. 为什么 Controller 耗时不能定位 Service 或 SQL 慢点

> 当前 AOP 只能说明一个 HTTP 接口整体用了多久，无法区分时间花在 Service、SQL、Redis 还是外部组件。生产环境还要增加方法级指标、数据库慢查询和连接池监控，或用 OpenTelemetry/SkyWalking 拆分 Span；当前项目没有做到这种粒度。

### 3. 请求参数、Token、手机号和评论内容怎样脱敏

> 当前请求日志只记录方法、URI、处理器、成功状态和耗时，没有打印参数、请求体、Token 或评论正文。生产中还应采用字段白名单和统一脱敏器，手机号只保留部分位数，Token 只记录哈希或短前缀，异常栈和 MQ 消息也要避免带出敏感内容。

### 4. 空值缓存 TTL 为什么更短，Bloom Filter 放在哪里

> 当前不存在的商户只缓存 2 秒，远短于正常商户的 30 分钟，因为空值只用于抑制穿透，还要避免新增数据被旧空值长期遮住。Bloom Filter 可放在回源 Redis/MySQL 前；它会把少量不存在的数据误判为可能存在，但不应把真实存在的数据判为不存在。当前项目没有落地 Bloom Filter。

### 5. 互斥重建中锁超时、重建失败和线程池饱和怎样处理

> 当前互斥版本抢锁失败后休眠并递归重试，没有重试上限；逻辑过期使用固定 10 线程池，但内部队列无界，重建异常只记日志。生产实现应使用有界退避与总超时、有界线程池和拒绝策略，重建失败时返回旧值并告警，避免锁或数据库异常演变成线程堆积。

### 6. 逻辑过期对象怎样首次预热

> 逻辑过期要求缓存中预先存在“业务数据 + 逻辑过期时间”，否则第一次查询仍没有旧值可返。当前已有写入逻辑过期对象的代码，但商户主查询没有启用，也没有定时预热；生产中可在启动、发布或定时任务中预热热点商户，并按访问热度刷新。

### 7. Redis 故障时能否直接回源数据库

> 当前 Caffeine 已命中的数据在本地 TTL 内还能返回，但 L1 未命中后访问 Redis 失败会使请求失败，没有受控的数据库回源降级。生产中不能让所有流量同时直打 MySQL，应先熔断 Redis、限制回源并发，结合热点旧值、本地缓存和单飞重建；非核心查询必要时直接降级。

### 8. Redis 预扣成功但队列入队失败怎样补偿

> 当前 `offer` 失败只返回“队列繁忙”，Redis 库存和一人一单资格不会恢复，这是明确缺口。生产方案应给预占记录增加唯一订单号和状态，确认入队失败后执行幂等补偿 Lua，原子恢复库存并删除资格；结果不确定时进入异常记录，由对账任务处理，不能盲目重复加库存。

### 9. Redis 与 MySQL 库存怎样对账

> 当前没有库存对账任务，而且 Redis 库存还包含“已预扣但未落库”的影响，不能简单要求它等于 MySQL 剩余库存。生产中应按券统计初始库存、成功订单、待处理预占和已补偿记录，计算期望值后比较两端；发现差异先告警并冻结活动，再用带幂等标识的任务修复。

### 10. 重复投递怎样保证只生效一次

> 当前本地队列没有重试，但数据库已有 `(user_id, voucher_id)` 唯一索引和订单主键，可拦住重复插入。升级为可靠 MQ 后，还应以 orderId 或 eventId 建消费记录或状态机，把“幂等检查、扣库存、写订单”放在同一数据库事务中，提交成功后再 ACK。

### 11. 秒杀接口怎样做限流、风控和防刷

> 当前异步入口没有校验活动起止时间，也没有限流、验证码和设备风控，只能作为功能验证。生产中应在网关和应用层按活动、用户、IP、设备多级限流，把活动状态和资格校验放进 Lua，并在高峰前启用动态路径或验证码；数据库唯一索引和条件扣减仍作最终兜底。

### 12. publisher confirm、mandatory/return 和持久化分别保证什么

> confirm 确认消息是否到达 exchange，mandatory/return 用于发现消息到达 exchange 后无法路由到任何队列。durable exchange 和 queue 保证拓扑重启后仍存在，消息还要使用 persistent delivery mode。当前拓扑是 durable，Spring AMQP 发出的消息默认 persistent，但没有 confirm/return、只有单 Broker，也没有完整消费幂等与补偿，所以仍不是可靠投递闭环。

### 13. 手动 ACK、重试队列、TTL 和死信怎样组合

> 当前监听容器使用 `acknowledge-mode=AUTO`，方法正常返回后由容器确认，没有显式重试和死信配置。生产中可在数据库事务成功后手动 ACK；可恢复异常进入带 TTL 的重试队列并限制次数，超过阈值或遇到毒消息后进入死信队列，保留失败原因和原消息供告警与补偿。

### 14. 消费幂等键放 Redis 还是数据库

> Redis 判重快，但过期、故障或数据丢失后可能再次消费；数据库唯一约束更持久，也能与业务写入放进同一事务。当前通知表没有业务唯一键，生产中更适合用“接收人 + 通知类型 + 业务 ID + 版本”建立唯一约束，Redis 只做前置削峰，不作最终依据。

### 15. 消息积压怎样监控和处理

> 应同时监控 ready、unacked、最老消息年龄、生产/消费速率、失败率和死信数量，而不只看队列长度。积压时先确认瓶颈在消费者、数据库还是外部依赖，再受控扩消费者、调整 prefetch，并对非核心生产流量限速；当前项目没有这套监控和自动扩缩容。

### 16. Flink CDC 需要哪些前置条件，Schema 变化怎样处理

> 当前 CDC 账号需要 `SELECT、RELOAD、SHOW DATABASES、REPLICATION SLAVE、REPLICATION CLIENT` 权限，并依赖 MySQL ROW 格式 binlog；项目 SQL 使用静态字段定义。当前没有自动 Schema 演进能力，不兼容 DDL 可能使作业失败；生产中应采用版本化 DDL、先加后删的兼容变更、灰度重提和 Savepoint 回滚。

### 17. Kafka 分区数怎样选择，扩缩容为什么会 Rebalance

> 当前 Topic 固定 3 分区、Flink 并行度 2，只是单机实验配置。生产中要按目标吞吐、单分区能力、Key 倾斜、消费并行度和扩容余量确定分区数；消费者或分区分配变化会触发 Rebalance，期间会短暂停顿，也可能在位点提交边界重复处理，因此下游仍要幂等。

### 18. 脏数据怎样存储和回放

> 当前非法行为写入 `dirty_behavior_event`，并按分钟把错误数量和样例写入 `ads_data_quality`，但没有自动修复和回放闭环。生产中应保留原始消息、错误原因、Schema 和作业版本，修正规则后把指定数据写入回放 Topic 或补数作业，并沿用原 eventId 保证下游幂等。

### 19. 窗口与 Watermark 怎样选择

> 当前主要按事件日期做持续日聚合，并非所有指标都使用固定窗口；Watermark 固定为观测事件时间减 10 秒，只是本地演示参数。滚动窗口适合固定周期统计，滑动窗口适合趋势排行，会话窗口适合访问会话；生产 Watermark 应根据乱序延迟分位数、结果时效和修正成本确定。

### 20. Flink 状态为什么膨胀，Checkpoint、Savepoint 和 RocksDB 怎样取舍

> 去重、Distinct、窗口和 Join 都会按 Key 保存状态，Key 持续增长或 TTL 过长会使状态膨胀。当前 DWD TTL 为 2 天、DWS 为 8 天，并使用 RocksDB；核心 DWD 作业以 10 秒 Checkpoint 做过恢复演练。RocksDB 适合较大状态但增加磁盘 I/O；Checkpoint 偏自动容错，Savepoint 偏人工升级迁移，当前未完成 DWS 历史重放或 Savepoint 升级回滚演练。

### 21. Doris 三种 Key 模型、分区和分桶怎样选

> Duplicate Key 保留明细，Unique Key 保留同一主键的最新值，Aggregate Key 适合规则固定的预聚合。当前明细和日汇总使用 Unique Key Merge-on-Write，以适配 CDC 更新和恢复重放；表没有日期分区，只有 1 或 3 个 Hash Bucket。生产中应按日期分区做裁剪和生命周期管理，再按主键分桶，并结合节点数、数据量和倾斜调整桶数。

### 22. 商户热度、内容排行和优惠券漏斗口径是什么

> 当前商户热度为“访问 UV + 净点赞数 × 3 + 订单数 × 5”，博客热度为“浏览 UV + 净点赞数 × 3”，均按自然日计算；这些权重是演示口径，尚未做刷量识别和业务校准。漏斗按日期、券和商户统计曝光、请求、受理、订单、支付，请求率是请求/曝光，下单率是订单/请求，支付率是支付订单/订单；Lua 受理成功不等于最终下单成功。

### 23. 怎样把本地秒杀队列升级成可靠消息链路

> 当前 Lua 预扣后进入 JVM 内存队列，宕机会丢任务。生产升级可把资格预占记录和订单命令写入 Redis Stream、RabbitMQ 或 outbox，消费者用订单 ID 幂等落库，事务提交后 ACK；重试耗尽进入死信，最终失败再执行幂等补偿，并提供订单状态查询和定期对账。

### 24. Kafka 或 Flink 长时间不可用时业务是否受影响

> Flink 停止时，只要 Kafka 仍可接收事件，主要影响是 Lag 增长和看板变旧，核心业务可继续。Kafka 长时间不可用则不同：`KafkaTemplate.send()` 可能等待元数据或缓冲空间，并同步抛运行时异常；当前发布器没有捕获它，可能拖慢或打断请求，回调失败也只记日志。生产中应隔离发布异常，并增加 outbox 或本地持久缓冲、容量告警和降级采样。

### 25. 实时指标怎样回放、补数、版本切换和审计

> 当前有消费位点、Checkpoint、恢复和对账脚本，但没有完整的历史 DWS 补数与无损切换流程。生产中应保留不可变 ODS，给事件 Schema、作业和指标口径分别编号，补数先写影子表并对账，再切换 ADS 视图；同时记录输入范围、代码版本、执行人和结果差异。

### 26. 业务接口和实时链路怎样分别压测

> 业务接口应观察 QPS、P95/P99、错误率、线程池、连接池、Redis/MySQL 和秒杀队列深度；实时链路则观察输入速率、Kafka Lag、Flink busy/backpressure、Checkpoint、Doris 可见延迟和最终对账。当前约 986 events/s 只覆盖脚本直接写 Kafka 到 Doris DWD，不包含 Spring Boot HTTP，不能当作业务接口 QPS。

### 27. 学习项目上线最先补哪三项可靠性能力

> 第一，补齐秒杀可靠队列、状态查询、重试、补偿和对账；第二，给 RabbitMQ 增加 outbox、confirm、手动 ACK、幂等和死信；第三，补 Redis/MQ 高可用、熔断限流和统一监控告警。当前功能链路和实时实验可运行，但这三项决定它能否从学习项目走向可运营系统。

### 28. `afterCompletion` 与 `finally` 分别适合在哪里清理上下文

> Web 请求上下文由拦截器写入时，适合在 `afterCompletion` 统一移除，因为正常返回和异常结束都会进入该回调；普通方法、线程池任务和 MQ 消费逻辑没有这个生命周期，应在各自 `finally` 中清理。两者目的相同：确保线程被复用前没有遗留用户或 MDC 数据。

### 29. `EVALSHA` 与 `EVAL` 有什么区别

> `EVAL` 每次发送脚本文本；`EVALSHA` 只发送脚本 SHA1，脚本已缓存时网络开销更小。若服务端返回 `NOSCRIPT`，客户端需要回退到 `EVAL` 或重新 `SCRIPT LOAD`。Spring `DefaultRedisScript` 会处理脚本缓存细节；两者的原子执行语义相同。

### 30. direct、topic 和 fanout exchange 怎样选择

> direct 按 routing key 精确匹配，适合当前统一社交通知；topic 支持通配符，适合按业务域和事件类型订阅；fanout 忽略 routing key，适合广播。选择依据是路由语义，不是性能口号；当前项目只有一个 durable direct exchange、一条 routing key 和一个队列。

### 31. Kafka Producer 的 `acks`、重试和幂等怎样配置

> 当前行为 Producer 使用 `acks=all`、3 次重试和异步回调，但没有 outbox 或本地持久缓冲。生产中还应结合合适的 `min.insync.replicas`、开启幂等生产、限制在途请求和设置交付超时；这些只能降低 Kafka 内部的丢失与重复，不能把业务数据库事务一起原子提交。

### 32. CDC 表与行为流关联时怎样选 Join

> 普通 Join 会长期保留两侧状态，流数据持续增长时成本高；Interval Join 适合“两类事件必须在某个时间范围内发生”的关系；Temporal Join 适合按事件发生时刻查询版本化维表。当前主 DWS 没有把券 CDC 与行为流做完整 Join，不能把这些方案说成已落地；若做漏斗，优先按稳定业务 ID 聚合事实，再按需要补充维度。

### 33. 维表更新后历史聚合是否重算

> 取决于指标口径。若要“按事件发生时的维度”，应保留版本并做 temporal join，历史结果不随当前维度变化；若要“按最新维度”，则需要回放或修正历史聚合。当前券主数据只做质量校验，没有实现维度版本化和历史重算。

### 34. 为什么用 Doris，而不是继续在 MySQL 上做看板

> MySQL 负责高并发事务读写，不适合持续扫描大量明细并做多维聚合；Doris 是列式分析引擎，支持分区裁剪、向量化执行、预聚合和高并发分析查询，更适合作为看板查询层。当前数据量很小，选型价值主要是验证 OLTP 与 OLAP 解耦，不能用本地样本证明生产收益。

### 35. 指标口径变化怎样版本化

> 事件 Schema、Flink 作业和指标定义应分别带版本；新口径先写影子表，与旧口径并跑和对账，再切换 ADS 视图。若历史数据要重算，应从保留的 ODS 按指定时间范围回放，并记录代码版本、输入范围和差异。当前项目只有固定演示公式，没有完整指标版本平台。

### 36. 看板的延迟、并发和数据新鲜度目标是什么

> 当前只测了 DWD 可见性，不是完整看板 SLA，也没有多并发查询压测，因此不能给出生产目标。正式目标应拆成事件进入 Kafka、Flink 处理、Doris 可见和接口查询四段，同时定义 P95/P99、新鲜度水位、超时降级和“数据延迟”提示，再据此做容量规划。

### 37. 怎样让二级缓存最终一致链路可观察

> 每次更新应记录数据库提交、L1 删除、L2 删除和广播结果，并为删除失败设置重试或 binlog 补偿；监控缓存版本、失效延迟、重试次数和抽样比对差异。当前只有日志、Pub/Sub 和 TTL，没有可靠重试和一致性指标，所以只能说明故障窗口，不能证明长期误差率。

### 38. 行为数据怎样做隐私、保留周期和访问控制

> 采集前先做最小化，只保留指标需要的用户或设备标识，并使用不可逆映射或独立密钥做伪匿名；敏感扩展字段采用白名单。Kafka、Doris 和日志分别设置保留周期、最小权限、传输与静态加密，并记录查询审计。当前是模拟数据和单机环境，没有完成生产级隐私治理。

### 39. 行为量增长百倍时怎样扩展 Kafka、Flink 和 Doris

> 先用压测确定单分区、单并行实例和单 BE 的真实上限，再按峰值及冗余扩 Kafka 分区与 Broker、Flink 并行度和 TaskManager、Doris BE 与分桶；同时检查 Key 倾斜、状态 TTL、Checkpoint I/O 和热点商户。扩分区会改变 Key 到分区的映射，扩状态作业要用 Savepoint 重分配，Doris 还需重新评估分区和桶数，不能只把所有并行度乘一百。

### 40. 锁业务执行时间超过 leaseTime 会怎样

> 显式指定 leaseTime 时，Redisson 通常不会启用 watchdog；业务尚未完成但租期已到，锁会自动释放，其他线程就可能进入临界区，原线程随后解锁还可能因已非持有者而失败。当前项目未指定 leaseTime，由 watchdog 续期，但仍以数据库唯一索引、条件扣减和事务作最终兜底。

---

## 五、与项目直接相关的 10 个高频 Java 八股

### 1. HashMap 和 ConcurrentHashMap 的结构与并发差别是什么

> JDK 8 的 HashMap 是数组 + 链表 + 红黑树，链表长度达到 8 且数组容量至少 64 时才树化；扩容为两倍后，节点位置要么不变，要么移动旧容量。它没有并发保护，并发写和扩容可能丢数据。ConcurrentHashMap 在 JDK 8 用 CAS 插入空桶、对桶头 `synchronized` 处理冲突写，关键字段用 `volatile` 保证可见性，并支持协助扩容。它只能保护单 JVM 内的 Map，不能代替项目中的 Redis 分布式锁。

### 2. Java 线程池的核心参数、执行流程和拒绝策略是什么

> 核心参数是 corePoolSize、maximumPoolSize、keepAliveTime、时间单位、workQueue、ThreadFactory 和 RejectedExecutionHandler。任务先创建核心线程，再入队；队列满后才创建非核心线程，线程数到最大且队列也满时触发拒绝。JDK 自带 AbortPolicy、CallerRunsPolicy、DiscardPolicy 和 DiscardOldestPolicy，业务也可自定义。生产中要使用有界队列、自定义线程名、拒绝日志和监控。项目的秒杀任务本身使用有界 `ArrayBlockingQueue`，但缓存重建用 `newFixedThreadPool(10)`，其内部队列无界，是需要继续改造的风险点。

### 3. ThreadLocal 为什么会串数据或泄漏，项目怎样清理

> 数据实际放在线程自己的 `ThreadLocalMap` 中，Entry 的 key 是 ThreadLocal 弱引用，value 是强引用。在线程池中线程长期存活，如果业务忘记 `remove()`，旧 value 可能被后续请求读到，也可能长期无法回收。项目在 JWT 拦截器的 `afterCompletion` 中清理用户上下文，在 TraceID 拦截器中清理 MDC；异步线程仍应显式传参，并在 `finally` 清上下文。

### 4. JVM 内存区域和一次 OOM 的排查流程是什么

> 线程私有区域有程序计数器、虚拟机栈和本地方法栈；线程共享区域有堆和方法区，JDK 8 的方法区由元空间实现；NIO 还会使用直接内存。排查 OOM 先确认是 heap、metaspace、direct memory、native thread 还是容器 OOMKilled，再保留 GC 日志、线程栈和 heap dump，用 MAT 看大对象、Dominator Tree 和 Path to GC Roots，最后结合代码和压测验证修复。项目里尤其要关注百万容量订单队列、无界线程池队列和静态线程池生命周期，但不能在没有实测时说已经发生过 OOM。

### 5. Spring IoC、AOP、Bean 生命周期和事务代理怎样理解

> Spring 先把配置解析为 BeanDefinition，再实例化、依赖注入，执行 Aware、BeanPostProcessor 前置、初始化方法和后置处理；AOP 代理通常在后置处理阶段生成。Spring 可对接口使用 JDK 动态代理，也可用 CGLIB 生成目标类子类。AOP 用代理抽取日志和事务等横切逻辑：项目的 Controller AOP 记录耗时和异常，订单事务则依赖 Spring 代理。同类直接 `this.method()` 会绕过代理，所以项目注入了懒加载的自身接口代理，再调用带 `@Transactional` 的落库方法。

### 6. 一次 Spring MVC 请求怎样到达 Controller 并返回

> 请求先进入 Servlet 容器和 Filter，再到 DispatcherServlet；HandlerMapping 找到处理器并取得 HandlerAdapter，随后先执行拦截器 `preHandle`。HandlerAdapter 调用参数解析器：Path、Query、Header 等走类型转换，`@RequestBody` 才由 HttpMessageConverter 处理，JSON 通常交给 Jackson。Controller 返回后由返回值处理器和消息转换器写出响应，最后执行 `afterCompletion`。项目拦截器顺序是 TraceID、JWT、登录校验，AOP 包围 Controller 方法记录成功标志和耗时。

### 7. MySQL 联合索引、回表、覆盖索引和索引下推是什么

> InnoDB 主键索引叶子存整行，二级索引叶子存索引列和主键；通过二级索引再查主键索引叫回表。查询需要的列都在二级索引中就是覆盖索引，可以避免回表；索引下推是在存储引擎扫描联合索引时提前过滤可判断的条件，减少回表。联合索引按最左列优先排序，设计时要结合等值、范围、排序和返回列，而不是只看字段区分度。

### 8. InnoDB 的 MVCC、锁、undo/redo/binlog 怎样协同

> undo log 保存旧版本，既支持事务回滚，也为 MVCC 版本链服务；Read View 根据事务 ID 判断哪个版本可见，RC 通常每次一致性读生成新视图，RR 通常第一次一致性读后复用。当前读和写的锁类型取决于隔离级别、索引与条件：RR 下范围当前读常用 Next-Key Lock，唯一索引等值命中通常退化为记录锁，RC 通常不使用间隙锁。redo log 通过 WAL 保证已提交修改可恢复，binlog 用于复制和时间点恢复，两者通过两阶段提交保持一致。项目本地事务只能覆盖库存扣减和订单插入，不能自动覆盖 Redis 与队列。

### 9. Redis 为什么快，RDB/AOF 和高可用分别解决什么

> Redis 的核心优势是内存访问、高效数据结构、主命令执行路径串行化配合 I/O 多路复用，减少锁竞争；但大 Key、慢命令、持久化和网络仍可能造成阻塞。RDB 是周期快照，恢复快但可能丢失快照后的数据；AOF 记录写命令，数据窗口更小但文件和恢复成本更高。主从提供副本，Sentinel 做故障转移，Cluster 做分片扩容；异步复制下主从切换仍可能丢最新写入。项目当前是单 Redis 学习环境，不具备这些生产高可用能力。

### 10. Redis 分布式锁怎样正确加锁、续期和解锁

> 手写锁至少要用 `SET key uniqueValue NX PX ttl` 原子加锁，value 标识持有者；解锁用 Lua 先比较 value 再删除，避免误删别人的锁。Redisson进一步用 Hash 和重入次数实现可重入，并在未指定 leaseTime 时由 watchdog 自动续期。分布式锁只能降低并发冲突，不能保证业务最终正确；项目仍用 Lua 资格校验、数据库条件扣减、唯一索引和事务多层兜底。

---

## 六、与项目直接相连的 17 个补充高频题

### 1. `volatile` 能保证什么，为什么 `count++` 仍不安全

> `volatile` 能保证共享变量的可见性，并限制特定指令重排，但不能保证复合操作的原子性。`count++` 包含读取、加一和写回，多线程可能同时读到旧值并覆盖结果。单变量计数可用 Atomic 类；项目中的库存和资格属于跨进程复合操作，应使用 Redis Lua 或数据库条件更新。

### 2. 线程池任务异常为什么可能被忽略

> `execute()` 的未捕获异常会交给线程的 `UncaughtExceptionHandler`，工作线程通常随后退出；`submit()` 会把异常封装进 `Future`，不调用 `get()` 就可能完全看不到。后台消费者应统一捕获、记录并告警，或使用 `afterExecute`、Future 回调和存活监控；当前秒杀消费者只记录异常，没有重试和自动恢复闭环。

### 3. 线程池怎样优雅关闭

> 先调用 `shutdown()` 停止接收新任务，让队列中任务继续执行，再用 `awaitTermination()` 有限等待；超时后才调用 `shutdownNow()` 尝试中断。任务代码还要正确响应中断，并持久化或移交未执行任务。当前秒杀是 JVM 本地队列，应用退出时仍存在任务丢失风险。

### 4. Filter、Interceptor、AOP 和 ControllerAdvice 怎样选

> Filter 位于 Servlet 容器入口，适合跨域、请求包装和最外层处理；Interceptor 能拿到 Handler，适合 JWT、登录上下文和 TraceID；AOP 适合方法级日志、事务等横切逻辑；ControllerAdvice 适合统一参数校验和异常响应。项目用 Interceptor 处理身份与上下文，用 AOP 记录 Controller 耗时。

### 5. Spring 事务的传播、回滚和 `afterCommit` 怎样理解

> 默认传播行为是 `REQUIRED`：有事务就加入，没有就新建；默认只对 `RuntimeException` 和 `Error` 回滚，受检异常要显式配置 `rollbackFor`。事务范围应尽量短，避免把外部 MQ 调用长时间包在数据库事务里。`afterCommit` 只能避免回滚后误发，不能消除“数据库已提交、消息尚未发送时宕机”的窗口。

### 6. Spring Boot 自动配置的原理是什么

> `@EnableAutoConfiguration` 从自动配置清单加载候选配置类，再通过 `@ConditionalOnClass`、`@ConditionalOnProperty`、`@ConditionalOnMissingBean` 等条件决定是否注册 Bean。依赖和配置满足条件才生效，用户自定义 Bean 时默认配置通常退让。它减少样板配置，但关键线程池、Redis、MQ 和数据库参数仍要显式审查。

### 7. B+Tree 为什么适合数据库索引

> B+Tree 分支多、树高低，一个磁盘页能保存大量键，因此查询所需 I/O 次数少；叶子节点有序并相连，也适合范围查询、排序和顺序扫描。哈希结构适合等值查找，却不擅长范围、排序和联合索引的前缀匹配。

### 8. `EXPLAIN` 重点看哪些字段

> 我会重点看 `type、key、key_len、rows、filtered、Extra`。`type=ALL` 通常表示全表扫描，`key_len` 可辅助判断联合索引用到哪些列，`rows` 是预估扫描量；`Using temporary`、`Using filesort` 或大量回表要重点分析。执行计划还要结合慢查询、锁等待和真实数据分布，不能只看单个字段下结论。

### 9. 哪些情况容易导致索引失效

> 常见原因包括跳过联合索引最左列、在索引列上做函数或运算、隐式类型转换，以及 `LIKE '%xxx'` 的前导模糊查询。范围条件之后的列通常不能继续用于缩小索引扫描范围。即使索引可用，命中行太多或回表成本过高时，优化器也可能选择全表扫描。

### 10. 数据库幂等怎样设计

> 插入类业务优先使用稳定业务唯一键，例如秒杀订单的 `(user_id, voucher_id)` 唯一索引；更新类业务使用状态机或条件更新，例如只允许“待处理”状态迁移。消息消费再用订单号或事件 ID 去重，并配合重试、补偿和对账。分布式锁只能减少并发冲突，不能替代数据库最终约束。

### 11. MySQL 死锁怎样形成和处理

> 两个事务以不同顺序持有并等待对方所需的锁，就可能形成循环等待。InnoDB 会检测死锁并回滚其中一个事务，应用应捕获异常并有限重试整个事务。预防重点是统一加锁顺序、缩短事务、让条件命中索引并避免一次锁住过多记录。

### 12. Redis 的 String、Hash、Set、ZSet 在项目中怎样使用

> String 用于商户缓存、验证码和简单状态；Hash 保存 refreshToken 对应的用户摘要；Set 保存关注关系和秒杀资格；ZSet 用时间戳作 score，支持点赞顺序和 Feed 滚动分页。选择依据是是否需要去重、排序、范围查询和字段级访问，而不是把所有数据都序列化成 JSON String。

### 13. Redis 过期删除和内存淘汰有什么区别

> 过期删除处理已经到 TTL 的 Key，Redis 主要采用惰性删除加定期抽样，因此到期不代表该毫秒立即释放内存。达到 `maxmemory` 后，`noeviction` 会拒绝新的写命令；LRU、LFU、random、volatile-ttl 等淘汰策略才会删除 Key。策略要结合数据是否允许丢失选择，不能把订单资格等关键状态随意配置为可淘汰。

### 14. 消息可靠投递为什么要分三段回答

> 生产端需要 confirm、发送回调和有限重试；Broker 需要持久化消息与队列，并配置副本或镜像；消费者应业务成功后再 ACK，失败进入有界重试和死信。这样通常得到至少一次投递，所以还必须用业务唯一键、状态机或数据库约束保证消费幂等。项目当前 RabbitMQ 尚未补齐这些能力。

### 15. 消息积压、重复、乱序和毒消息分别怎样处理

> 积压先比较生产与消费速率，再按 Queue 或 Partition 数扩消费者；消费者数超过可并行分片后继续增加没有收益。重复用业务 ID 幂等，同一业务键的顺序消息路由到同一 Queue 或 Partition 并串行处理。不可恢复的格式或参数错误属于毒消息，有限重试后应进入死信并告警补偿。

### 16. Kafka 的 ISR、`acks`、幂等生产者和消费组分别解决什么

> ISR 是与 Leader 保持同步的副本集合；`acks=all` 配合 `min.insync.replicas` 能降低 Leader 故障时丢消息的风险。幂等生产者可避免网络重试在同一 Partition 写出重复记录，Kafka 事务能原子提交一组 Kafka 写入和消费位点，但不能自动覆盖 MySQL。消费组内一个 Partition 同一时刻只由一个消费者处理，因此并行度上限受 Partition 数限制；当前项目只是单 Broker 学习环境。

### 17. MyBatis 的 `#{}`、`${}` 与 SQL 注入有什么区别

> `#{}` 使用 PreparedStatement 占位符，参数作为数据绑定，不能改变 SQL 结构；`${}` 是原始字符串替换，MyBatis-Plus 的 `.last()` 也会直接拼接 SQL，必须使用白名单或先转成可信类型。项目的 `ORDER BY FIELD(id, ...)` 使用 `.last()`，但 ID 已先转为 `Long`，避免了直接拼接任意用户字符串；仍应把这类写法列入代码审查重点。

---

## 七、比赛两题：现有证据不足，只保留可填模板

### Q1. MoE Block 单卡训练显存峰值为什么能降低约 60%

**可口述模板：**

> 这次任务是在 **[设备型号]** 上训练 **[Block/参数规模]**，我先固定 batch、序列长度、精度和输入数据，用 **[测量工具]** 把显存拆成参数、梯度、优化器状态、激活和临时 buffer。基线峰值是 **[A GB]**。我实际采用了 **[混合精度/激活重计算/分块/offload/释放中间张量等真实动作]**，每一步都在相同条件下复测；最终峰值降到 **[B GB]**，相对下降约 60%，代价是 **[吞吐或训练时间变化]**，前向/反向误差为 **[验证结果]**。比赛成绩应说 **[个人/团队] 前 50%**。

**没有填完前的安全说法：**

> 我完成了 MoE Block 的单卡显存剖析和优化实验，但 60% 的等价条件、绝对显存和性能代价还需要以原始日志确认，因此面试时不会只报一个百分比。

### Q2. 多轮 Text-to-SQL 数据怎样构建和校验，你个人做了什么

**可口述模板：**

> 任务环境中 Ascend 910B、VERL 和 Qwen3.5-2B 的实际关系是 **[待按比赛材料核实]**。我的个人职责是 **[按证据填写：数据清洗/SQL 校验/多轮样本构建]**，不把团队训练工作算成个人成果。每条样本的真实字段为 **[schema、历史轮次、当前问题、目标 SQL、执行结果等]**；我用 **[实际规则]** 处理重复和冲突，并按 **[数据库/schema/模板]** 隔离数据集。SQL 安全和正确性实际采用 **[parser/AST/正则/沙箱/超时/执行结果比对]**；只有确实做过 parser、隔离库执行和 execution accuracy，才能口述这些能力。最终是团队 8/18；我能证明的个人交付物是 **[脚本、数据版本、规则和评测报告]**，实际 VERL 算法与奖励是 **[待核实]**。

---

## 八、面试时最容易说错的 18 句话

1. 不说“生产系统”，说“个人学习项目，在本地单机/容器环境完成验证”。
2. 不说“全链路追踪”，说“单体 HTTP 请求级 TraceID 日志”。
3. 不说“普通请求会查 Redis 会话”，当前 accessToken 鉴权只解析 JWT。
4. 不说“退出后 Token 立即失效”，当前旧 accessToken 仍能活到过期。
5. 不说“商户查询同时用了空值、互斥锁和逻辑过期”，当前主链路只启用空值缓存。
6. 不说“Pub/Sub 保证缓存消息不丢”，它没有持久化和补发。
7. 不说“秒杀用了 RabbitMQ”，当前是 JVM 本地阻塞队列。
8. 不说“秒杀受理就是订单成功”，最终成功必须以 MySQL 订单为准。
9. 不说“异步秒杀已校验活动时间”，当前主入口没有 begin/end 校验。
10. 不说“Redis 预扣成功后的异常已有补偿”，当前入队、抢锁和落库失败都没有完整补偿。
11. 不说“RabbitMQ exactly-once”，当前连 confirm、幂等和死信都未补齐。
12. 不说“Feed 同分滚动分页已正确处理”，当前 offset 累计分支写反，修复并补测后再讲闭环。
13. 不说“整条实时链路绝对 exactly-once”或“生产可达 986 events/s”，只能引用固定单机环境的实测结果和边界。
14. 不说“`latency_ms` 是端到端延迟”，当前只计算 ingestTime 减 eventTime。
15. 不说“DAU 覆盖所有下单用户”，当前 DAU 只来自合法行为事件。
16. 不说“请求数等于 accepted + rejected”，队列满的 `ERROR` 当前不计入两者。
17. 不说“GMV 按支付日统计”，当前按订单 createTime 归属日期，退款金额另列。
18. 不说“properties 中的扩展字段都进入 DWD”，当前只显式提取 reason，followUserId 会丢失。

---

## 九、源码与实验依据速查

以下路径均相对于项目根目录 `E:\Java_learn\heimadianping\hm-dianping`：

- 认证：`src/main/java/com/hmdp/service/impl/UserServiceImpl.java`、`JwtTool.java`、`JwtTokenInterceptor.java`、`MvcConfig.java`。
- 请求级日志：`TraceIdInterceptor.java`、`RequestLogAspect.java`、`application.yaml`。
- 二级缓存：`LocalCacheConfig.java`、`CacheClient.java`、`CacheInvalidationSubscriber.java`、`ShopServiceImpl.java`、`CacheClientTest.java`。
- 秒杀：`seckill.lua`、`VoucherOrderServiceImpl.java`、`VoucherOrderController.java`、`hmdp.sql` 中的库存主键和一人一单唯一索引。
- Feed 与通知：`FollowServiceImpl.java`、`BlogServiceImpl.java`、`BlogCommentsServiceImpl.java`、`RabbitMqConfig.java`、`SocialNotificationProducer.java`、`SocialNotificationConsumer.java`。
- 实时分析：`analytics/`、`realtime-warehouse/flink-sql/10-dwd.sql`、`20-dws.sql`、`doris/01-schema.sql`、`INTERVIEW_QUESTIONS.md`。
- 实测证据：`realtime-warehouse/BENCHMARK_REPORT.md`、`benchmark-results/`、`scripts/run-benchmark.ps1`、`run-recovery-test.ps1`、`reconcile.ps1`。
