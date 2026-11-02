# 判定规则与阈值

## 数据倾斜

以 task 级数据为准：

- `recordsRatio = max(recordsRead) / median(recordsRead)`
- `durationRatio = max(duration) / median(duration)`
- `maxShare = max(recordsRead) / sum(recordsRead)`

判定：

- **确认倾斜**：task 数 ≥ 4，`recordsRatio ≥ 5` 且 `durationRatio ≥ 3` 且 `maxShare ≥ 0.2`；
- **疑似倾斜**：`recordsRatio ≥ 3` 或 `durationRatio ≥ 3`，但未同时满足；
- **不可判定**：task 数 < 4，分布没有统计意义。

注意：

- **不要只看 shuffle bytes**。热点 key 高度可压缩时，bytes 可能只高 2–3 倍，而 records 高几十倍；
- 一个慢 task 不代表倾斜：如果它的 records 正常、`executorDeserializeTime` 很高，是冷启动；
- 两个慢 task 也不一定都是倾斜：逐个看 records 和 deserialize。

## 冷启动 / 调度

- task 启动延迟：`launchTime - stage.submissionTime`；
- stage 墙钟：`completionTime - submissionTime`；
- `scheduling_gap = wall - max(task.duration)`。

判定：

- `max_launch_delay ≥ 1s` 或 `scheduling_gap > 0.5 * wall` 且 `scheduling_gap ≥ 3s` → 调度/冷启动；
- 慢 task 的 `deserialize / duration ≥ 0.3` 且 records 正常 → 冷 executor。

常见原因：

- `spark.dynamicAllocation.minExecutors=0`，空闲释放后重新申请；
- YARN 队列排队、节点资源不足；
- 首次任务支付类加载/JIT 成本。

## Spill / GC

- `memoryBytesSpilled` / `diskBytesSpilled` 在 stage 或 SQL 节点 `spill size`；
- `jvmGcTime / duration` 高（例如 > 20%）才考虑 GC 问题；
- 热点 task 的 sort spill 是倾斜的后果，优先修倾斜。

## 计划模式

| 计划特征 | 含义 |
|---|---|
| `HashAggregate(partial_count/partial_sum)` + Exchange | map 端预聚合，可能掩盖 GROUP BY 倾斜 |
| `count(DISTINCT)` 两阶段 | 第一次 shuffle key 包含 distinct 字段，热点被摊开 |
| `SortMergeJoin` | 两边都 shuffle，热点 key 会集中到 reduce 分区 |
| `BroadcastHashJoin` | 小表广播，通常没有 join 侧 shuffle 倾斜 |
| `Exchange hashpartitioning(key, 8192)` 但 shuffle 只有几 MB | 分区过量，AQE 可能 coalesce，但 block 开销已发生 |

## AQE

- `spark.sql.adaptive.skewJoin.enabled` 默认 true；
- 判定依据是**分区大小**，不是 records；
- 条件同时满足才 split：
  - 分区大小 > `skewedPartitionThresholdInBytes`（常见 256MB / 400MB）；
  - 分区大小 > `skewedPartitionFactor` × 中位数分区大小（常见 5）。
- 可压缩热点（大量相同 key）可能只有几 MB，**不会触发**；
- AQE 还可能 coalesce 分区，最终计划需要实际执行后看 `AQEShuffleRead coalesced` / `isFinalPlan=true`。

## Join 膨胀

- `join 输出行数 / 输入行数` 明显大于 1；
- 常见原因：join key 在维表重复、笛卡尔积、非等值 join；
- 输出行数等于事实表行数时，通常不是膨胀，而是分布问题。

## 分区数

- 增加 shuffle 分区数**不能**拆分单个 hot key；
- 单个 key 永远进一个分区；分区越多，其他分区越小，`max/median` 反而越大；
- 只有 salting、AQE skew split、广播/预聚合能改变单 key 分布。

## 建议优先级

1. 能广播的小表 → 广播；
2. 能预聚合的事实表 → 先聚合再 join；
3. 不能广播/预聚合 → salting；
4. 冷启动 → 热 executor / 固定 executor；
5. 分区过量 → 降低初始分区数，保留 AQE；
6. 不要只打开 AQE 就期待修复可压缩热点。
