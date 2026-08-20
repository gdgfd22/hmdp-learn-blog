# 实时数仓压测与故障恢复：30 秒 vs 10 秒 Checkpoint 调优全记录

> 摘要：Doris 2PC 的正式提交与 Flink Checkpoint 成功对齐，Checkpoint 间隔越长，数据等待提交可见的时间通常越长。本文记录我在单机 Docker Compose 环境对核心作业做的压测与故障恢复演练：把 Checkpoint 间隔从 30 秒调到 10 秒后，DWD 可见性 P95 上界从 36.05 秒降到 21.93 秒、最大 Kafka Lag 从 30,000 降到 9,991，代价是 Checkpoint 平均耗时从约 298 毫秒升到约 951 毫秒；模拟 TaskManager 故障后，核心 DWD 作业约 23.8 秒恢复运行、37.8 秒内完成新 Checkpoint、50.9 秒内 Lag 清零，2 万条事件无丢失。所有数字均来自固定单机环境，P95 为包含轮询查询开销的可见性上界，不能外推为生产容量。

## 一、为什么要这样做（业务背景与痛点）

实时看板的价值在于"近实时"。链路跑通之后，我发现数据在 Doris 的可见速度受两个因素制约。

第一，**Doris 2PC 提交与 Checkpoint 强相关**。Doris Sink 的正式提交发生在 Checkpoint 成功之后，间隔 30 秒意味着一条事件最坏要等接近一个 Checkpoint 周期才能被真正提交可见；Kafka 消费位点同样在 Checkpoint 完成时提交，间隔越长，Lag 的锯齿越高。

第二，**Lag 与可见性互相印证**。压测以 1,000 events/s 的目标速率持续写入，30 秒基线轮次里最大 Consumer Lag 顶到了 30,000，说明位点提交太慢，间接表明数据在链路里积压。

不解决会怎样：看板数字滞后，运营无法及时响应；故障恢复后 Lag 长期不回落，说明恢复质量可疑。于是我把"降低可见延迟"和"验证故障恢复"作为两个明确目标，用可复现的脚本压测并保存原始结果，避免凭感觉改参数。

## 二、用什么方法解决（方案对比）

### 1. Checkpoint 间隔：30 秒还是 10 秒？

| 方案 | 优点 | 缺点 |
|---|---|---|
| 30 秒基线 | Checkpoint 开销小（平均约 298 ms） | Doris 2PC 提交等待长，可见 P95 上界 36.05 秒、最大 Lag 30,000 |
| 10 秒调优 | 提交与位点推进更频繁，可见 P95 上界 21.93 秒、最大 Lag 9,991 | Checkpoint 平均耗时升到约 951 ms（+219.47%） |

调优内容不止改间隔：核心 DWD 和 DWS 作业的 Checkpoint 间隔 30 秒 → 10 秒，最小 Checkpoint 间隔 10 秒 → 3 秒，数据质量任务保持 30 秒（它不承担 2PC 可见性压力）。当前最大单轮 Checkpoint P95 为 2.54 秒，仍低于 10 秒触发间隔，说明开销尚在可控范围；但间隔不能无限缩短，否则快照和事务开销会反过来压低吞吐。

### 2. Kafka Source 启动位置：earliest 还是 group-offsets？

| 方案 | 问题 |
|---|---|
| 固定 `earliest-offset` | 无状态重提作业会重复读取全部历史，放大 DWD Changelog |
| `group-offsets` + `auto.offset.reset=earliest` | 已有消费组位点时从已提交位置继续；首次启动没有位点时才回退 earliest |

修复后，普通 ODS 和质量检测 Source 改为后者。DWS 使用 Upsert Kafka Source，Flink 1.20 的 Upsert Kafka 不支持 `scan.startup.mode` 选项，其自身按消费者组位点启动，因此只配置 `auto.offset.reset=earliest` 作为无已提交位点时的回退策略。

### 3. 恢复成功条件：只看 RUNNING 够吗？

最初只检查作业是否重新显示 RUNNING，实测发现 RUNNING 并不代表状态稳定。恢复脚本最终把成功条件收紧为五条：新 TaskManager 完成注册、DWD 作业恢复为 RUNNING、恢复后完成新的成功 Checkpoint、Kafka Lag 回落至 0、本轮事件全部可在 Doris 查询。

## 三、为什么需要这个技术（原理深入）

### 1. Checkpoint 与 2PC 的时序关系

Flink 触发 Checkpoint Barrier，Barrier 随数据流经过各算子，算子在一致位置快照状态，Source 位点和 Sink 事务参与同一次 Checkpoint，全部确认后才成功。Doris Sink 在 Checkpoint 周期内先预提交本批数据，Checkpoint 成功后再提交事务，失败时该批不作为成功结果提交。因此"数据在 Doris 可见"天然叠加一个 Checkpoint 周期的等待——这就是 30 秒 → 10 秒能直接压低可见性延迟的机制原因。

### 2. 指标口径（先讲清楚，再谈数字）

- **Flink Source 吞吐**：REST API 读取 `Source__ods_behavior_event.numRecordsOut` 增量除以生产端持续时间；
- **Doris 有效可见吞吐**：本轮事件总数除以"开始压测至全部事件可在 Doris 查询"的持续时间，包含 Checkpoint 等待与 2PC 提交，不是 Stream Load 瞬时吞吐；
- **DWD 可见性延迟上界**：Doris 可见行数每次变化时，读取本轮已可见事件计算"主机观测到可查询的时间 − event_time"，包含轮询、Docker Exec 和查询耗时，是上界，不是 ADS 看板接口延迟；
- **Kafka Lag**：消费者组 `hmdp-dwd-behavior` 3 个分区 Lag 之和，Flink 在 Checkpoint 完成时提交位点，因此 Lag 呈与 Checkpoint 周期相关的锯齿变化。

### 3. 30 秒基线 vs 10 秒调优（数字来自实测报告）

测试环境：Intel Core i5-13400、Docker 可用内存约 15.5 GB；Kafka 3.8.1 单 Broker（4 个 Topic、每 Topic 3 分区、副本 1）；Flink 1.20.1 单 JobManager + 单 TaskManager、并行度 2、Task Slot 16、RocksDB 状态后端；Doris 2.1.9 单 FE + 单 BE，Unique Key Merge-on-Write，Sink 开启 2PC。每轮 30,000 条行为事件，目标速率 1,000 events/s，用户数 5,000、商户数 14，事件 ID 带独立 run_id 前缀避免历史数据影响计数。

30 秒基线：

| 轮次 | 实际输入 events/s | Doris 有效 rows/s | 可见 P50 | 可见 P95 上界 | 最大 Lag | Checkpoint 平均耗时 | 失败 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 988.57 | 814.24 | 22.49 s | 36.05 s | 30,000 | 254.50 ms | 0 |
| 2 | 986.81 | 579.39 | 20.27 s | 33.82 s | 17,713 | 266.50 ms | 0 |
| 3 | 987.69 | 538.83 | 19.94 s | 35.89 s | 17,932 | 372.00 ms | 0 |
| 平均/最大 | 987.69 | 644.15 | 20.90 s | 36.05 s | 30,000 | 297.67 ms | 0 |

10 秒调优：

| 轮次 | 实际输入 events/s | Doris 有效 rows/s | 可见 P50 | 可见 P95 上界 | 最大 Lag | Checkpoint 平均耗时 | 失败 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 985.87 | 846.31 | 13.02 s | 18.32 s | 9,646 | 581.00 ms | 0 |
| 2 | 986.78 | 669.67 | 10.43 s | 19.61 s | 9,991 | 739.40 ms | 0 |
| 3 | 985.32 | 756.05 | 14.60 s | 21.93 s | 9,491 | 1,532.50 ms | 0 |
| 平均/最大 | 985.99 | 757.34 | 12.68 s | 21.93 s | 9,991 | 950.97 ms | 0 |

对比结论：平均实际输入速率 987.69 → 985.99 events/s（-0.17%，基本持平）；Doris 有效可见吞吐 644.15 → 757.34 rows/s（+17.57%）；DWD 可见 P95 上界 36.05 → 21.93 秒（-39.18%）；最大 Lag 30,000 → 9,991（-66.70%）；Checkpoint 平均耗时 297.67 → 950.97 ms（+219.47%）；两组正式压测 Checkpoint 失败均为 0。三组 90,000 条输入全部在 Doris 可见。

### 4. 故障恢复演练（核心 DWD）

在 500 events/s 持续输入下，等待一次成功 Checkpoint 后终止唯一 TaskManager 并重启：

| 指标 | 结果 |
|---|---|
| 输入事件 | 20,000 |
| 作业恢复为 RUNNING | 23.82 s |
| 恢复后首个成功 Checkpoint | 37.83 s |
| Kafka Lag 回落至 0 | 50.88 s |
| 最大 Kafka Lag | 15,124 |
| Doris 可见事件 / 数据丢失 | 20,000 / 0 |

恢复链路：TaskManager 恢复后，作业从最近可用 Checkpoint 恢复算子状态和 Kafka 位点并继续消费；脚本确认新 Checkpoint 成功、Lag 清零、数据完整后才判定恢复成功。**该结果只针对核心 DWD 链路，完整 DWS 历史重放的稳定恢复没有纳入，不应表述为"全链路 23 秒恢复"**。

### 5. MySQL 与 Doris 对账

抽样对账按指定日期分别查询 MySQL 订单表和 Doris `dws_order_day`，比较订单数、已支付订单数、GMV、退款金额：

| 指标 | MySQL | Doris | 差异 |
|---|---:|---:|---:|
| 订单数 | 40 | 40 | 0 |
| 支付订单数 | 30 | 30 | 0 |
| GMV（分） | 142,500 | 142,500 | 0 |
| 退款金额（分） | 0 | 0 | 0 |

不用"准确率 99.99%"这类模糊表述，直接说明对账字段及差异为 0；该结果只证明样本口径一致。

## 四、不用这个技术怎么办（替代方案与当前边界）

- **保持 30 秒 Checkpoint**：实现零改动，但可见性 P95 上界 36.05 秒、最大 Lag 30,000，看板滞后明显；
- **继续缩短 Checkpoint 间隔（如 5 秒）**：理论上可见更快，但快照与 2PC 事务开销会上升，本轮实测单轮 Checkpoint P95 已到 2.54 秒，间隔过短可能反噬吞吐——需要再压测验证，不能想当然；
- **不修 Kafka 启动位置**：每次重提作业都会从 earliest 全量回放历史，DWD Changelog 被放大，压测、对账和恢复演练都会被污染；
- **恢复只查 RUNNING**：会误判恢复质量，实测中 RUNNING 不代表状态稳定，必须叠加"新 Checkpoint 成功 + Lag 清零 + 数据完整"。

当前边界（诚实口径）：

- 单机 Docker Compose 环境：单 Kafka Broker（副本 1）、单 JobManager + 单 TaskManager、单 FE/BE、Flink 并行度 2、Kafka 3 分区、Task Slot 16、RocksDB 状态后端；
- 事件生成器直接写 Kafka，压测覆盖 `Kafka → Flink DWD → Doris DWD`，**不包含 Spring Boot HTTP 与应用埋点序列化耗时**；
- P50/P95 是 DWD 明细可见性上界，包含轮询、Docker Exec 和查询开销，**不是 ADS 看板接口 SLA，也不能外推为生产集群容量**；
- 恢复数字只针对核心 DWD 链路；DWS 全量历史重放的恢复时间未验证；还没有 Prometheus/Grafana 自动告警。

生产环境如何升级：多 Broker 与副本、Flink HA、Doris 多副本与按日期分区，按峰值压测确定分区数与并行度；Checkpoint 间隔、最小间隔与超时要结合状态大小、2PC 提交能力和分段 SLA 设定；监控覆盖 Flink、Kafka、Doris 与业务指标，把恢复演练纳入自动化。

## 小结

- Doris 2PC 提交与 Checkpoint 对齐，是可见性延迟的主要来源之一；10 秒间隔换来 P95 上界 36.05 → 21.93 秒、最大 Lag 30,000 → 9,991。
- 调优有代价：Checkpoint 平均耗时 297.67 → 950.97 ms，单轮 P95 已到 2.54 秒，间隔不能无限缩短。
- 故障恢复演练：RUNNING 23.82 s、新 Checkpoint 37.83 s、Lag 清零 50.88 s，2 万条事件无丢失——只覆盖核心 DWD。
- 恢复成功条件必须包含"新 Checkpoint + Lag 清零 + 数据完整"，只看 RUNNING 会误判。
- MySQL 与 Doris 对账：订单 40/40、支付 30/30、GMV 142,500/142,500、退款 0/0，差异均为 0。
- 所有数字来自固定单机环境：包含轮询查询开销的上界、脚本直写 Kafka 不含 HTTP，不能外推为生产容量或 SLA。
