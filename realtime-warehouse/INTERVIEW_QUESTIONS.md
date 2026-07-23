# 点评平台实时数仓面试问题与参考答案

> 适用项目：点评生活服务与实时分析平台
> 回答原则：先讲业务目标，再讲数据流和实现，最后讲取舍与不足。不要背定义，也不要编造吞吐量、延迟和性能提升数据。

## 一、面试前必须说准确的项目事实

### 1. 项目的真实数据链路是什么？

```text
Spring Boot 用户行为埋点 ──> Kafka ODS ──> Flink SQL DWD ──> Kafka DWD ──> Flink SQL DWS
                                          └───────────────> Doris DWD       └──> Doris DWS

MySQL 订单、优惠券表 ──> Flink CDC ───────> Kafka DWD / Doris DWD ─────────> Doris DWS

Doris DWS ──> Doris ADS View ──> Spring Boot 查询接口 ──> 实时看板
```

当前项目中，商户访问、博客浏览、点赞、关注、优惠券曝光和秒杀请求属于应用行为埋点，由 Spring Boot 写入 Kafka。订单和优惠券业务表由 Flink CDC 采集。

因此，不要说“用户、商户、优惠券和订单全部通过 Flink CDC 采集”。更准确的说法是：

> 行为事件通过应用埋点写入 Kafka，订单与优惠券业务表通过 Flink CDC 捕获 MySQL 变更，两类数据在 DWD 层完成标准化后进入实时聚合链路。

### 2. 30 秒项目介绍怎么说？

> 我在原有点评业务系统上增加了一套实时分析模块。用户行为由 Spring Boot 埋点写入 Kafka，订单和优惠券变更由 Flink CDC 从 MySQL 捕获。Flink SQL 在 DWD 层完成合法性校验、事件去重和脏数据分流，在 DWS 层按天及业务主题持续聚合，计算 DAU、商户 PV/UV、优惠券漏斗、订单量和 GMV 等指标。结果写入 Doris，通过 ADS 视图和 Spring Boot 接口提供给实时看板。同时配置了 Checkpoint、Doris 2PC、Kafka Lag 检查、数据质量任务以及 MySQL 与 Doris 对账脚本。

### 3. 简历中哪些话目前不建议写？

- 不要写“用户、商户、优惠券及订单全部由 Flink CDC 采集”。实际只有订单和优惠券表走 CDC。
- 不要写“Flink 完成复杂维表关联”。当前 DWS 主要使用事件自带的业务 ID 聚合，ADS 在 Doris 中关联不同主题汇总表。
- 不要写“端到端严格 Exactly-Once”而不解释边界。项目配置了 Flink Exactly-Once Checkpoint 和 Doris 2PC，但应用埋点生产端、Kafka、Flink、Doris整个链路的语义需要分段说明。
- 不要写具体 QPS、P95 延迟、日处理量和性能提升比例，除非完成并保存了可复现的压测记录。
- 不要把 `SECKILL_REQUEST result=ACCEPTED` 直接称为秒杀成功。它只表示 Lua 校验通过并进入异步处理，最终成功以 MySQL 中实际订单为准。

## 二、项目架构必问

### 4. 为什么要在 MySQL 和实时计算之间使用 Kafka？

参考回答：

> Kafka主要承担解耦和削峰作用。业务应用只负责发送行为事件，不需要等待下游聚合完成；Flink可以按照自己的消费能力处理数据。Kafka还保留可回放的事件日志，计算逻辑修改或任务恢复时可以重新消费。多个质量检测和聚合作业也能通过不同消费者组独立消费同一主题。

继续追问时可以补充：Kafka 不能替代业务库；业务最终状态仍以 MySQL 为准，Kafka承载的是事件流和变更流。

### 5. 为什么行为数据直接写 Kafka，订单数据使用 Flink CDC？

> 浏览、曝光、点赞等行为天然是事件，未必都需要先落业务表，直接写 Kafka 延迟更低，也避免给 MySQL 增加大量写压力。订单属于核心业务事实，需要先由事务保证业务正确性，所以以 MySQL 为事实源，再通过 CDC 捕获状态变化。

### 6. 为什么选择 Flink SQL，而不是全部使用 DataStream API？

> 当前任务以过滤、去重、分组聚合和结果写出为主，关系表达清晰，Flink SQL 开发成本更低，也便于查看执行计划。对于复杂自定义状态、异步 I/O、无法用 SQL 表达的业务规则或精细定时器控制，我会再使用 DataStream API。

### 7. 为什么选择 Doris，而不是直接用 MySQL 做看板查询？

> MySQL主要服务事务型读写，不适合让多维聚合和排行榜查询持续占用业务库资源。Doris面向分析查询，能够承接明细和汇总数据，支持按业务键更新以及对聚合结果进行低延迟查询，使在线业务与分析负载隔离。

### 8. 为什么没有同时使用 Hive、Spark、HBase？

> 这个项目的目标是完成一条可运行、可恢复、可校验的实时链路，所以技术范围控制在 Kafka、Flink和 Doris。Hive、Spark更适合补充离线数仓、历史重算和批处理能力，但在当前数据规模下同时引入会增加复杂度，且无法体现实现深度。

### 9. Docker Compose 环境能否直接用于生产？

> 不能。当前是单 Kafka、单 Flink JobManager、单 TaskManager、单 Doris FE/BE 的本地学习与故障演练环境，副本数也设置为 1。生产环境需要考虑多节点高可用、数据副本、资源隔离、权限认证、监控告警、容量规划以及跨机故障恢复。

## 三、数仓分层与指标口径

### 10. ODS、DWD、DWS、ADS 分别负责什么？

- **ODS**：保存原始行为事件和 MySQL CDC 变更，尽量保持源数据结构。
- **DWD**：校验字段和业务状态，生成统一用户标识，按 `event_id` 去重，并将合法数据与脏数据分流。
- **DWS**：按平台、用户、商户、博客、优惠券和订单主题维护日粒度聚合。
- **ADS**：组合 DWS 指标，为看板输出总览、排行榜、转化漏斗和留存结果。

### 11. 为什么要分层，直接一条 SQL 算到 ADS 不行吗？

> 技术上可以，但会导致清洗规则、指标口径和展示逻辑耦合。分层后，DWD 可以被多个下游复用，DWS 统一公共口径，ADS只负责面向应用组合指标。出现数据问题时，也能按 ODS、DWD、DWS逐层定位。

### 12. PV、UV、DAU 是怎么计算的？

> PV是满足条件的事件次数，例如 `SHOP_VIEW` 每出现一次就累加一次。商户 UV 是按日期和商户分组后，对 `user_key` 去重计数。DAU 是某天产生有效行为的 `user_key` 去重数。登录用户使用 `u:userId`，匿名用户使用 `d:deviceId`，避免两种标识直接冲突。

示例：同一用户当天访问同一商户两次，商户 PV 增加 2，UV 只增加 1。

### 13. `COUNT(DISTINCT user_key)` 在大数据量下有什么问题？

> 精确去重需要维护较大的状态，用户基数增加后会消耗大量内存和 RocksDB 状态空间，并使 Checkpoint 变大。当前项目为了保证指标直观采用精确去重。规模扩大后，可以根据业务容忍度使用 HyperLogLog 等近似去重，或者做分桶去重和两阶段聚合。

### 14. 优惠券漏斗的口径是什么？

当前 ADS 漏斗按“日期 + 优惠券 + 商户”关联行为与订单主题：

```text
曝光数 -> 秒杀请求数 -> 实际订单数 -> 已支付订单数
```

- 请求率 = 秒杀请求数 / 曝光数
- 下单率 = 实际订单数 / 秒杀请求数
- 支付率 = 已支付订单数 / 实际订单数

分母为 0 时返回 0，避免除零错误。

### 15. 秒杀接受率和秒杀成功率有什么区别？

> 接受率的分子是 Lua 校验通过并进入异步队列的请求，它反映入口校验结果；成功率的分子是 MySQL 中最终生成的订单数，它还受到异步消费、数据库写入等后续环节影响。项目看板最终成功率使用实际订单数除以秒杀请求数。

### 16. GMV 是如何计算的？退款怎么处理？

> 当前 GMV 统计存在支付时间的订单 `pay_amount` 之和，金额统一以“分”为单位，避免浮点误差。退款金额单独汇总为 `refund_amount`，没有直接从 GMV 中扣除；如果业务需要净收入，应单独定义 `net_gmv = gmv - refund_amount`，不能混用口径。

### 17. 热门商户和热门博客如何排序？

> 当前 ADS 使用可解释的加权分数。商户热度综合访问 UV、净点赞数和订单数；博客热度综合浏览 UV 与净点赞数。权重属于业务规则，不是通用标准，生产中应由产品和运营确认，并进行版本管理。

### 18. 次日和 7 日留存如何计算？

> 先从用户日活表得到每个 `user_key` 的首次活跃日期，形成 cohort；再判断该用户是否在首次活跃后的第 1 天和第 7 天再次出现。留存率分别是对应回访人数除以 cohort 人数。

### 19. 当前为什么按自然日聚合，而不是滚动窗口？

> 看板指标的业务口径是自然日，所以当前按照事件时间格式化后的日期分组并持续更新日累计值。滚动窗口更适合“最近 5 分钟访问量”等实时趋势。如果加入分钟级趋势，我会使用事件时间滚动窗口并明确允许迟到范围。

## 四、Flink 与 Flink SQL 高频问题

### 20. Flink CDC 的工作原理是什么？

> MySQL CDC首次启动时可以读取表的存量快照，随后持续读取 binlog 中的新增、修改和删除事件，并将它们转成 Flink Changelog。项目配置为 `initial` 启动模式，订单和优惠券表都声明了业务主键。

可能追问：CDC账号需要 `SELECT`、`RELOAD`、`SHOW DATABASES`、`REPLICATION SLAVE`、`REPLICATION CLIENT` 等权限，MySQL需要开启 binlog 并使用 row 格式。

### 21. `PRIMARY KEY NOT ENFORCED` 是什么意思？

> 它告诉 Flink 规划器该字段在数据语义上是主键，但 Flink不会像关系型数据库一样主动检查唯一性。正确性由上游数据保证。主键信息使 Upsert Kafka 和 Doris Unique Key Sink 能够识别同一业务记录的更新。

### 22. 为什么 DWD 使用 Upsert Kafka？

> 订单 CDC 和去重后的行为数据不是只有追加语义，同一主键可能发生更新。Upsert Kafka 用消息 Key 表示业务主键，新值覆盖旧值，删除可用墓碑消息表达，适合作为持续变化的 DWD 表。

### 23. 行为事件如何去重？

> DWD 对合法行为按 `event_id` 分区，使用 `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts)`，只保留第一条。`event_id` 由应用生成 UUID，并在 Doris DWD 表中作为 Unique Key，形成计算层去重加存储层幂等更新两层保护。

需要主动说明：流式去重会占用状态，因此项目设置了状态 TTL。TTL 到期后非常晚到的重复事件可能再次被当成新事件，生产环境应根据最大重放周期和存储成本确定 TTL。

### 24. Event Time、Ingest Time、Process Time 有什么区别？

- **Event Time**：业务事件实际发生时间，适合业务窗口和历史回放。
- **Ingest Time**：事件进入埋点采集链路的时间，可用于估计上游传输延迟。
- **Process Time**：Flink算子处理数据时的机器时间，受任务负载和重启影响。

项目用 Event Time 归属统计日期，并保存 Event、Ingest、Process 三类时间用于分析延迟。

### 25. Watermark 有什么作用？项目如何设置？

> Watermark表示系统认为事件时间已经推进到哪里，用于判断事件时间窗口何时可以触发以及哪些数据属于迟到数据。ODS行为表设置了 `event_ts - 10秒` 的 Watermark，表示允许一定程度的乱序。

注意：当前主要 DWS 是持续更新的日累计分组，不是一个会在 Watermark 到达后关闭的滚动窗口；不能把 Watermark 的作用夸大成所有聚合都依赖它触发。

### 26. Checkpoint 是什么？项目如何配置？

> Checkpoint是 Flink对算子状态和数据源消费位置制作的一致性快照。项目核心 DWD、DWS作业每 10 秒执行一次，数据质量任务保持 30 秒；模式为 Exactly-Once，超时 5 分钟，同时只允许一个 Checkpoint进行。状态存储在共享 Docker Volume中，并保留取消任务后的外部化 Checkpoint。10 秒间隔来自压测调优，降低了 Doris 2PC提交等待，但增加了 Checkpoint开销。

### 27. Checkpoint 和 Savepoint 有什么区别？

> Checkpoint主要由系统自动触发，用于故障恢复；Savepoint通常由用户主动触发，用于版本升级、迁移和计划内停机。两者都保存状态，但生命周期和运维目的不同。

### 28. 为什么使用 RocksDB 状态后端？

> UV去重、流式去重和持续聚合都可能维护较多状态。RocksDB可以把状态放在本地磁盘和内存组合中，容量通常比纯堆内状态更大，代价是序列化和磁盘访问开销。项目环境中配置 `state.backend.type: rocksdb`，主要用于演示有状态任务和恢复。

### 29. 项目能保证 Exactly-Once 吗？

建议分段回答：

> Flink内部配置了 Exactly-Once Checkpoint；Kafka Source 的消费位点随 Checkpoint 一起保存；Doris Sink 开启 2PC，并且 Doris表使用 Unique Key。这样可以降低故障恢复时重复写入的风险。但端到端语义还取决于 Kafka生产端配置、CDC源能力、Sink事务提交和恢复方式，所以我不会笼统声称整条链路在任何情况下都是绝对 Exactly-Once。

### 30. Doris Sink 的 2PC 如何理解？

> Sink在 Checkpoint期间先预提交本批数据，Checkpoint成功后再提交事务；如果 Checkpoint失败则不会把该批数据作为成功结果提交。它把外部写入与 Flink Checkpoint 对齐。Doris Unique Key还能让同一业务主键的重复写入表现为覆盖更新。

### 31. Flink作业失败后如何恢复？

> 项目配置了 failure-rate 重启策略。TaskManager故障后先恢复容器，作业从最近可用 Checkpoint 恢复算子状态和 Kafka消费位置。恢复后检查 Checkpoint、Kafka Lag和 Doris结果，再运行 MySQL与 Doris对账脚本。Checkpoint Volume如果被删除，就不能再声称从原状态恢复。

### 32. 什么是反压？如何在 Flink UI 中判断？

> 下游算子处理速度跟不上时，输出缓冲区逐步占满，压力会向上游传播，这就是反压。Flink UI 中主要看 Back Pressure、Busy、Idle、吞吐和各 Subtask差异。某节点持续高反压时，应继续沿下游查找真正的慢算子，而不是只调整上游。

### 33. 看到某个 Sink 的 `Records Sent = 0`，能否说明没有写入？

> 不能直接下结论。部分外部 Sink 的提交过程不会完整反映在标准 `Records Sent` 指标中。应结合 Records Received、Checkpoint是否成功、Doris Load/事务状态以及目标表数据共同判断。

### 34. 并行度如何设置？当前项目为什么是 2？

> 当前并行度 2 是本地演示配置，不是容量规划结果。生产中需要结合 Kafka分区数、算子 CPU与 I/O 开销、状态大小、目标吞吐和 Sink能力压测决定。Source并行度通常受 Kafka分区数约束，KeyBy后的数据分布还会影响下游负载。

### 35. 如何发现和处理数据倾斜？

> 先比较同一算子各 Subtask 的 Busy、Records和 Backpressure。如果少数 Subtask明显更忙，通常说明某些 Key过热。可以增加分区、对热点 Key加盐做两阶段聚合、拆分热点业务，或者改进分区键。盲目增加并行度不一定能解决单个热点 Key问题。

### 36. 状态 TTL 为什么 DWD 是 2 天，DWS 是 8 天？

> DWD的 TTL主要约束事件去重状态，当前假设两天后到达的重复事件不再需要保持精确去重；DWS包含用户活跃和 7 日留存相关状态，因此保留时间更长。它们是演示环境的取值，生产中需要根据迟到范围、重放周期、留存口径和状态成本确定。

## 五、Kafka 高频问题

### 37. Topic、Partition、Consumer Group 分别是什么？

> Topic是消息的逻辑分类；Partition是并行读写和顺序性的基本单位，同一 Partition内有序；Consumer Group表示一组协作消费者，同组内一个 Partition同一时刻只由一个消费者处理，不同消费者组可以独立读取同一 Topic。

### 38. Kafka消息的 Key 应该如何选择？

> 需要保证同一业务实体有序时，应选择稳定业务 ID作为 Key，例如 `event_id`、`order_id` 或 `voucher_id`。但也要评估热点，Key过于集中会造成单分区压力。当前 Upsert Kafka必须通过主键表达更新语义。

### 39. 什么是 Consumer Lag？Lag升高如何排查？

> Lag是分区最新 Offset与消费者已提交或已处理 Offset之间的差值。排查顺序包括：消费者是否存活、Flink是否反压、各分区是否倾斜、Checkpoint是否长期失败、下游 Doris是否变慢、网络和磁盘是否异常。恢复后还要观察 Lag是否持续回落。

### 40. Kafka能保证消息不丢吗？

> 要看完整配置和故障范围。生产端需要确认 ACK、重试和幂等配置；Broker需要合理副本数和 `min.insync.replicas`；消费者要正确管理 Offset。当前本地 Kafka副本数为 1，只适合演示，不能抵御节点级数据丢失。

### 41. 为什么不同 Flink任务使用不同 Consumer Group？

> DWD清洗、DWS聚合和质量检测是不同消费目的，需要各自完整读取主题。如果共用同一 Group，分区会在任务之间分配，导致每个任务只能读到部分数据。

普通 Kafka Source使用 `group-offsets` 启动模式，并配置无已提交位点时回退到 earliest。DWS使用的 Upsert Kafka Source自身按消费者组位点启动，只配置 `auto.offset.reset=earliest`作为首次启动回退。这样重复提交时可以从已提交位点继续，避免固定使用 earliest造成全量历史反复回放。

## 六、Doris 高频问题

### 42. 项目为什么使用 Unique Key 模型？

> CDC记录和实时聚合结果会持续更新，同一个 `event_id`、订单 ID或“日期 + 业务维度”会多次写入。Unique Key Merge-on-Write适合按业务主键覆盖最新值，也能降低任务恢复造成的重复结果。

### 43. Doris 的分桶键为什么选择业务主键？

> 明细表按事件 ID或订单 ID Hash分桶，汇总表按商户、博客、优惠券等核心维度分桶，希望让数据相对均匀分布。当前桶数只有 1或 3，是单节点演示配置；生产中的桶数应根据节点数、数据量、Tablet大小和查询模式规划。

### 44. DWS表和 ADS视图有什么区别？

> DWS表保存可复用的主题聚合结果，如商户日汇总、优惠券日汇总和订单日汇总。ADS使用 Doris视图将这些结果按看板口径关联和计算，例如排行榜权重、漏斗转化率和留存率。这样修改展示公式时不必重算所有 DWD明细。

### 45. Doris和 ClickHouse如何选择？

> 两者都适合分析查询。当前项目更需要 CDC结果和聚合结果的按键更新，Doris Unique Key模型及 Flink Doris Connector比较契合。ClickHouse在追加写和高性能分析方面也很强，但更新模型、分布式表设计和运维方式不同。选择应以更新频率、查询模式、团队经验和运维成本为依据，而不是只比较单项跑分。

### 46. 看板查询为什么通过 Spring Boot，而不是前端直接连 Doris？

> 数据库不应直接暴露给浏览器。Spring Boot接口负责参数校验、权限控制、口径封装和返回结构转换，也便于后续增加缓存、限流和审计。

## 七、数据质量与对账

### 47. 项目做了哪些数据质量检查？

- 行为事件：空事件 ID、非法事件类型、缺少用户标识、缺少业务 ID、缺少时间、未来时间和过期事件。
- 重复事件：按分钟和 `event_id` 统计重复数量。
- 订单：关键外键为空、非法状态、负金额、支付加优惠超过原价、退款超过实付、状态与支付/退款时间不一致。
- 优惠券：商户为空、非法类型、非法状态、优惠金额关系不合法。

异常行为写入 Kafka脏数据主题；按分钟聚合的质量结果写入 Doris `ads_data_quality`。

### 48. 为什么既要实时质量检测，又要离线对账？

> 实时检测能快速发现单条数据的格式和业务规则问题，但不能证明最终指标一定正确。对账从业务事实源与分析汇总两端独立计算同一口径，可以发现 CDC漏数、任务积压、聚合错误和写入异常。两者解决的问题不同。

### 49. MySQL与 Doris如何对账？

> 项目按指定日期分别查询 MySQL订单表和 Doris `dws_order_day`，比较订单数、已支付订单数、GMV和退款金额。出现差异时先检查 Kafka Lag、Checkpoint状态和非法订单质量记录，再判断是数据尚未到达还是计算口径错误。

### 50. 数据质量规则会不会误杀正常数据？

> 会，所以规则必须可解释、可配置并保留脏数据样例。项目没有直接删除无效行为，而是分流到脏数据 Topic并记录错误原因。生产中还应加入规则版本、告警阈值和人工回放机制。

## 八、故障与场景题

### 51. Kafka Lag持续上涨，但 Flink没有明显反压，可能是什么原因？

- Source并行度小于有效分区数或部分分区发生倾斜。
- 消费者发生频繁 Rebalance。
- Checkpoint失败导致位点不能稳定推进。
- 作业实际读取了错误 Topic、Group或启动位置。
- 指标采集存在延迟，需要结合输入吞吐和 Broker状态确认。

### 52. Flink反压很高，怎么定位？

> 从出现反压的算子沿数据流向下游查找，比较各算子的 Busy、吞吐和 Subtask差异。如果 Doris Sink最慢，检查 FE/BE状态、导入事务、批次和网络；如果聚合算子最慢，检查热点 Key、状态大小和 RocksDB I/O；如果只有一个 Subtask异常，优先怀疑数据倾斜。

### 53. Checkpoint突然变慢或失败，怎么排查？

- 查看失败原因、持续时间、对齐时间和状态大小。
- 检查是否存在反压，Barrier无法及时对齐。
- 检查 RocksDB和 Checkpoint Volume磁盘空间、I/O与权限。
- 检查外部 Sink预提交是否超时。
- 判断状态是否异常增长，例如精确 UV或去重状态 TTL不合理。

### 54. Flink恢复后 Doris出现重复数据怎么办？

> 先确认是否从最近 Checkpoint恢复、Doris 2PC是否启用、Sink Label是否稳定，以及目标表是否为正确的 Unique Key。明细表以事件或订单 ID作为 Unique Key，汇总表以日期和业务维度作为 Unique Key，可以将重复写表现为覆盖。如果使用 Duplicate Key或无主键追加表，就需要额外幂等设计。

### 55. 看板数字暂时不变化，应该检查什么？

1. 业务应用是否成功发送埋点。
2. Kafka Topic是否有新消息，Consumer Lag是否变化。
3. Flink任务是否 Running，Source是否收到记录。
4. 聚合 Sink的 Checkpoint和 Doris事务是否成功。
5. Doris目标日期数据是否更新。
6. Spring Boot查询日期、时区和接口参数是否正确。

### 56. 如果业务要求修改历史指标口径，如何重算？

> 当前 Kafka保留期内可以使用新的 Consumer Group从指定 Offset回放行为数据；订单也可以重新执行 CDC初始快照或从业务库抽取。生产中更稳妥的方案是保留可重算的明细层或离线历史层，将新结果写入新版本表，完成对账后再切换 ADS，避免直接覆盖线上口径。

## 九、压力追问与诚实回答模板

### 57. 这个项目的数据量有多大？延迟是多少？

> 在 Kafka 3分区、Flink并行度 2 的单机 Docker环境中，我执行了 3 轮、每轮 3 万条事件的测试。10 秒 Checkpoint配置下，平均实际输入约 986 events/s，9 万条数据全部写入 Doris，Checkpoint平均耗时约 951 ms且正式压测期间无失败。DWD可见性 P95上界约 21.9 秒；这里的上界包含轮询和查询开销，不是 ADS看板 SLA，也不能外推为生产集群性能。

继续追问故障恢复时：

> 在 500 events/s持续输入下终止唯一 TaskManager，核心 DWD作业约 23.8 秒恢复 RUNNING、37.8 秒内产生新的成功 Checkpoint、50.9 秒内 Lag回落到 0，2 万条测试事件全部写入 Doris。这个数字只针对核心 DWD链路，不描述为完整 DWS全链路恢复。

### 58. 这是不是只把几个组件用 Docker跑起来？

> 不只是启动组件。我实现了业务埋点、CDC表、DWD合法性校验和去重、脏数据分流、多个 DWS主题聚合、Doris表模型与 ADS视图、查询接口、实时看板、质量任务、健康检查和 MySQL-Doris对账。同时我也清楚当前是单节点演示环境，与生产高可用部署还有差距。

### 59. 项目最难的地方是什么？

可以选择自己真正理解最深的一点回答，例如：

> 最难的是统一“秒杀请求被接受”和“最终订单成功”的业务口径。入口埋点只能说明 Lua校验通过，最终订单需要以 MySQL CDC数据为准。因此漏斗把请求事件与实际订单分开建模，避免用入口结果虚增秒杀成功率。

或：

> 另一个难点是处理更新流。订单状态会变化，DWS结果不是单纯追加，所以使用 Upsert Kafka、Flink Changelog和 Doris Unique Key模型传递更新，并通过 Checkpoint与 2PC降低恢复后的重复写风险。

### 60. 项目目前最大的不足是什么？

> 第一，基础设施是单节点，不能代表生产高可用；第二，精确 UV在大基数下状态成本较高；第三，目前主要是日累计指标，分钟窗口和迟到数据修正还可以继续加强；第四，虽然已有可复现压测报告，但还没有 Prometheus、Grafana和自动化告警；第五，DWS全量历史重放的吞吐和恢复时间仍需继续优化。

### 61. 如果让你继续优化，会怎么做？

优先级建议：

1. 针对 DWS全量回放和多 Sink写入继续进行吞吐、状态大小及恢复时间优化。
2. 增加 Prometheus、Grafana和告警规则，覆盖 Flink、Kafka、Doris与业务指标。
3. 增加分钟级窗口、迟到数据处理和指标版本管理。
4. 针对高基数 UV评估 HyperLogLog或分桶两阶段聚合。
5. 引入 Schema Registry或事件契约，保证埋点字段兼容。
6. 补充多节点高可用、权限控制和敏感配置管理。

## 十、现场 SQL 手写题

### 62. 计算每日商户 PV、UV

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

### 63. 按事件 ID 去重，只保留第一条

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

### 64. 计算优惠券请求率、下单率和支付率

```sql
SELECT
    exposure_count,
    request_count,
    order_count,
    paid_order_count,
    CASE WHEN exposure_count = 0 THEN 0
         ELSE request_count * 100.0 / exposure_count END AS request_rate,
    CASE WHEN request_count = 0 THEN 0
         ELSE order_count * 100.0 / request_count END AS order_rate,
    CASE WHEN order_count = 0 THEN 0
         ELSE paid_order_count * 100.0 / order_count END AS pay_rate
FROM voucher_funnel_source;
```

### 65. 计算次日留存

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

## 十一、面试复习顺序

如果准备时间有限，按以下顺序复习：

1. 能在 30 秒内准确讲清数据链路和两类数据源。
2. 能解释 DWD、DWS、ADS每层的输入、处理和输出。
3. 能手写 PV/UV、漏斗和去重 SQL。
4. 能解释 Flink CDC、Changelog、Checkpoint、状态后端和反压。
5. 能解释 Kafka Partition、Consumer Group和 Lag。
6. 能解释 Doris Unique Key、分桶和 2PC。
7. 能说明数据质量、对账和故障恢复步骤。
8. 能诚实说明单节点、精确 UV和未正式压测等项目边界。

## 十二、对应项目文件

- DWD清洗、CDC和去重：`flink-sql/10-dwd.sql`
- 数据质量任务：`flink-sql/11-quality-behavior.sql` 至 `14-quality-duplicate.sql`
- DWS聚合：`flink-sql/20-dws.sql`
- Doris表模型与 ADS视图：`doris/01-schema.sql`
- Kafka、Flink和 Doris健康检查：`scripts/check-health.ps1`
- MySQL与 Doris抽样对账：`scripts/reconcile.ps1`
- 测试事件生成：`scripts/generate-events.ps1`
- 本地环境配置：`docker-compose.yml`
