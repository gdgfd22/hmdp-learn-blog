# 实时数仓压测与故障恢复报告

测试日期：2026-07-23

## 1. 结论

在单机 Docker Compose 环境中，使用 Kafka 3 分区、Flink 并行度 2，分别对 30 秒和 10 秒 Checkpoint配置执行 3 轮压测，每轮持续产生 30,000 条行为事件，目标速率为 1,000 events/s。

核心结论：

- 10 秒配置下，3 轮共 90,000 条事件全部写入 Doris，平均实际输入速率为 985.99 events/s。
- Flink Source在可取得完整计数的轮次中，处理速率与生产速率一致，平均为 986.05 events/s。
- DWD明细写入 Doris的整体有效可见吞吐由 644.15 rows/s提升至 757.34 rows/s，提高 17.57%。
- DWD事件可见性 P95上界由 36.05 秒下降至 21.93 秒，下降 39.18%。
- 最大 Kafka Consumer Lag由 30,000下降至 9,991，下降 66.70%。
- Checkpoint平均耗时由 297.67 ms增加至 950.97 ms；两组正式压测期间均无 Checkpoint失败。
- 核心 DWD作业故障演练中，TaskManager终止后 23.82 秒恢复为 RUNNING，37.83 秒内完成新的成功 Checkpoint，50.88 秒内 Lag回落至 0；20,000 条测试事件无丢失。
- MySQL与 Doris的订单数、支付订单数、GMV和退款金额对账差异均为 0。

当前配置采用 10 秒 Checkpoint。压测结果来自本地单节点环境，不能外推为生产集群容量。

## 2. 测试环境

| 项目 | 配置 |
|---|---|
| CPU | Intel Core i5-13400，10 核 16 线程 |
| 物理内存 | 31.78 GB |
| Docker可用内存 | 约 15.5 GB |
| Docker版本 | 28.3.0 |
| Kafka | 3.8.1，单 Broker，KRaft |
| Kafka Topic | 4 个，每个 3 分区，副本数 1 |
| Flink | 1.20.1，单 JobManager + 单 TaskManager |
| Flink并行度 | 2 |
| Task Slot | 16 |
| 状态后端 | RocksDB |
| Doris | 2.1.9，单 FE + 单 BE |
| Doris表模型 | Unique Key Merge-on-Write |
| Doris Sink | 2PC |

## 3. 工作负载

每轮数据参数：

- 事件数：30,000
- 目标生产速率：1,000 events/s
- 用户数：5,000
- 商户数：14
- 事件类型：商户访问、博客访问、点赞、优惠券曝光、秒杀请求、关注
- 事件 ID：使用独立 `run_id` 前缀，避免历史数据影响计数

事件生成器直接写入 Kafka，因此结果覆盖 `Kafka -> Flink DWD -> Doris DWD`，不包含 Spring Boot HTTP请求和应用埋点序列化耗时。

## 4. 指标口径

### 4.1 Flink Source吞吐

使用 Flink REST API读取 `Source__ods_behavior_event.numRecordsOut`，以本轮 Source计数增量除以生产端持续时间。

### 4.2 Doris有效可见吞吐

以本轮事件总数除以“开始压测至全部事件可在 Doris查询”的持续时间。该指标包含等待 Checkpoint和 Doris 2PC提交的时间，不等同于 Stream Load瞬时吞吐。

### 4.3 DWD可见性延迟上界

每当 Doris可见行数发生变化，读取本轮已可见事件并计算：

```text
主机观测到数据可查询的时间 - event_time
```

该结果包含轮询、Docker Exec和查询耗时，因此作为实际可见性延迟的上界。它是 DWD明细可见性指标，不代表 ADS看板接口响应延迟。

### 4.4 Kafka Lag

采集消费者组 `hmdp-dwd-behavior` 的 3 个分区 Lag之和。Flink在 Checkpoint完成时提交消费位点，因此 Lag会呈现与 Checkpoint周期相关的锯齿变化。

## 5. 30 秒 Checkpoint基线

| 轮次 | 实际输入 events/s | Flink Source events/s | Doris有效 rows/s | DWD可见 P50 | DWD可见 P95上界 | 最大 Lag | Checkpoint平均耗时 | Checkpoint失败 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 988.57 | 988.57 | 814.24 | 22.49 s | 36.05 s | 30,000 | 254.50 ms | 0 |
| 2 | 986.81 | 986.81 | 579.39 | 20.27 s | 33.82 s | 17,713 | 266.50 ms | 0 |
| 3 | 987.69 | 987.69 | 538.83 | 19.94 s | 35.89 s | 17,932 | 372.00 ms | 0 |
| **平均/最大** | **987.69** | **987.69** | **644.15** | **20.90 s** | **36.05 s** | **30,000** | **297.67 ms** | **0** |

原始结果：

- [JSON](benchmark-results/baseline-30s/benchmark-20260723-172302.json)
- [CSV](benchmark-results/baseline-30s/benchmark-20260723-172302.csv)
- [汇总](benchmark-results/baseline-30s/benchmark-20260723-172302-summary.json)

## 6. 10 秒 Checkpoint调优结果

调优内容：

- DWD和 DWS核心作业 Checkpoint间隔由 30 秒改为 10 秒。
- 核心作业最小 Checkpoint间隔由 10 秒改为 3 秒。
- 数据质量任务保持 30 秒间隔。

| 轮次 | 实际输入 events/s | Flink Source events/s | Doris有效 rows/s | DWD可见 P50 | DWD可见 P95上界 | 最大 Lag | Checkpoint平均耗时 | Checkpoint失败 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 985.87 | 指标初始化中 | 846.31 | 13.02 s | 18.32 s | 9,646 | 581.00 ms | 0 |
| 2 | 986.78 | 986.78 | 669.67 | 10.43 s | 19.61 s | 9,991 | 739.40 ms | 0 |
| 3 | 985.32 | 985.32 | 756.05 | 14.60 s | 21.93 s | 9,491 | 1,532.50 ms | 0 |
| **平均/最大** | **985.99** | **986.05** | **757.34** | **12.68 s** | **21.93 s** | **9,991** | **950.97 ms** | **0** |

原始结果：

- [JSON](benchmark-results/optimized-10s/benchmark-20260723-173416.json)
- [CSV](benchmark-results/optimized-10s/benchmark-20260723-173416.csv)
- [汇总](benchmark-results/optimized-10s/benchmark-20260723-173416-summary.json)

## 7. 调优前后对比

| 指标 | 30 秒基线 | 10 秒配置 | 变化 |
|---|---:|---:|---:|
| 平均实际输入速率 | 987.69 events/s | 985.99 events/s | -0.17% |
| Doris有效可见吞吐 | 644.15 rows/s | 757.34 rows/s | +17.57% |
| DWD可见 P95上界 | 36.05 s | 21.93 s | -39.18% |
| 最大 Kafka Lag | 30,000 | 9,991 | -66.70% |
| Checkpoint平均耗时 | 297.67 ms | 950.97 ms | +219.47% |
| 正式压测 Checkpoint失败 | 0 | 0 | 不变 |

降低 Checkpoint间隔显著改善了 Doris 2PC提交等待和消费位点提交频率，但增加了 Checkpoint开销。当前最大单轮 Checkpoint P95为 2.54 秒，仍低于 10 秒触发间隔。

## 8. 核心 DWD故障恢复

在 500 events/s持续输入期间，等待一次成功 Checkpoint后终止唯一 TaskManager并重新启动。最终成功条件同时包含：

1. 新 TaskManager完成注册。
2. DWD作业恢复为 RUNNING。
3. 恢复后完成新的成功 Checkpoint。
4. Kafka Lag回落至 0。
5. 本轮事件全部可在 Doris查询。

| 指标 | 结果 |
|---|---:|
| 输入事件 | 20,000 |
| 目标生产速率 | 500 events/s |
| 作业恢复为 RUNNING | 23.82 s |
| 恢复后首个成功 Checkpoint | 37.83 s |
| Kafka Lag回落至 0 | 50.88 s |
| 最大 Kafka Lag | 15,124 |
| Doris可见事件 | 20,000 |
| 数据丢失 | 0 |

原始结果：[recovery result](benchmark-results/recovery-dwd-10s/recovery-20260723174821-c91a6c-result.json)

该结果针对核心 DWD链路。完整 DWS历史重放的稳定恢复没有纳入该数字，不应在简历中表述为“全链路 23 秒恢复”。

## 9. 数据准确性

### 9.1 压测事件

- 30 秒基线：90,000 条输入，Doris可见 90,000 条。
- 10 秒配置：90,000 条输入，Doris可见 90,000 条。
- 最终 DWD恢复测试：20,000 条输入，Doris可见 20,000 条。

### 9.2 MySQL与 Doris订单对账

| 指标 | MySQL | Doris | 差异 |
|---|---:|---:|---:|
| 订单数 | 40 | 40 | 0 |
| 支付订单数 | 30 | 30 | 0 |
| GMV（分） | 142,500 | 142,500 | 0 |
| 退款金额（分） | 0 | 0 | 0 |

不使用“准确率 99.99%”等模糊表述，直接说明对账字段及差异为 0。

## 10. 测试中发现并修复的问题

### 10.1 Doris 2PC Label冲突

重新提交使用固定 `sink.label-prefix` 的作业时，历史已完成 Label可能与新作业冲突。提交脚本现为每次部署追加唯一 Deployment ID，避免新作业与历史事务共用 Label。

### 10.2 Kafka历史重复回放

普通 Kafka Source原先固定使用 `earliest-offset`，无状态重提会重复读取全部历史并放大 DWD Changelog。当前 ODS和质量检测 Source改为：

```text
scan.startup.mode = group-offsets
properties.auto.offset.reset = earliest
```

已有消费者组位点时从已提交位置继续；首次启动没有位点时才回退到 earliest。

DWS使用 Upsert Kafka Source。Flink 1.20的 Upsert Kafka不支持 `scan.startup.mode`选项，其自身按消费者组位点启动，因此只配置 `properties.auto.offset.reset = earliest`作为无已提交位点时的回退策略。

### 10.3 恢复成功条件过宽

最初只检查作业是否重新显示 RUNNING。实际测试发现，RUNNING并不代表状态已稳定。因此恢复脚本增加“恢复后新 Checkpoint成功、Lag清零和数据完整”三个条件。

## 11. 简历可用表述

推荐拆成两条：

> 在 Kafka 3 分区、Flink并行度 2 的单机 Docker环境下完成 3 轮共 9 万条行为事件压测，平均处理速率约 986 events/s，测试数据全部写入 Doris；通过 MySQL与 Doris对账验证订单数、支付订单数及 GMV差异为 0。

> 针对 Doris 2PC提交延迟，将核心作业 Checkpoint间隔由 30 秒调整为 10 秒，使 DWD可见性 P95上界降低 39.2%、最大 Kafka Lag降低 66.7%；模拟 TaskManager故障后，核心 DWD作业 23.8 秒恢复运行、37.8 秒内完成新 Checkpoint，2 万条事件无丢失。

面试时必须说明以上结果来自本地单节点环境，P95为包含轮询开销的 DWD可见性上界，并非生产集群 SLA。
