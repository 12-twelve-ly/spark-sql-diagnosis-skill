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

## Stage 慢因判定顺序

先看 task 分布，再看资源，再看计划：

1. **数据倾斜**：max records / duration 远大于中位数，最大 task 占比高；
2. **map 端倾斜**：map 阶段 input records / bytes 分布不均，常见于不可切分文件、
   大量小文件、异常宽记录；
3. **单 task 过重**：task 均衡但单 task 输入行数/字节过大，整体都慢；
4. **shuffle fetch 慢**：fetchWait / remote reqs 占 task 时间比例高，上游 task 多
   或 block 太小；
5. **并行度受限**：
   - executor 数接近 maxExecutors，task launch 间隔大 → 被上限卡住；
   - executor 数低于上限或申请不到资源 → 队列/集群资源不足；
6. **调度/冷启动**：stage 墙钟远大于最长 task，launch delay 高；
7. **stage 重试/失败**：失败 attempt、fetch failure、节点异常。

默认阈值（可在代码里调）：

- 倾斜：records ≥5x、duration ≥3x、最大 task 占比 ≥20%；
- 并行度：task ≥50、launch p90-p10 ≥2s；
- 单 task 过重：中位输入 >1000 万行或 >512MB；
- fetch：fetchWait 或 remote reqs 占 task 总时长 ≥20%；
- 小文件：文件数 ≥100 且平均 <64MB；
- job/stage 过多：job >20 或 stage >50。

## SQL 与 Stage 映射

- 每个 shuffle 算子（group by、join、distinct、repartition、window、order by）
  都会切出 stage 边界；
- 边界前是 map/write，边界后是 reduce/read；
- 计划节点的 metrics 里常带 `(stage X.Y: task N)`，可据此建立 stage → 节点的映射；
- Exchange 的 hash key 通常就是 group by / join 的 key；
- 报告应写出“Stage 37 → SortMergeJoin / Exchange(item_id)”，而不是只说
  “某个 stage 慢”。

## 解法矩阵

| 场景 | 首选 | 约束/风险 |
|---|---|---|
| 大表 join 小表 | 广播/map join | left join 只能广播右表，right join 只能广播左表，full join 不适用；小表压缩后解压可能膨胀，注意 executor 内存 |
| 大表 join 大表，少量 hot key | 拆出 hot key，hot 侧广播，非 hot 侧正常 join，再 union | 只适合少量热点；热点值要能提前识别 |
| 大表 join 大表，大量 hot key | 随机前缀 + 膨胀另一侧 | 内存开销大，属于缓解 |
| null 值倾斜 | 空值加随机后缀，保持不匹配语义 | 不要破坏 left join 语义 |
| group by 倾斜 | 两阶段聚合：hot key 加盐，局部聚合，去盐再聚合 | 盐值常用 50–100；纯 rand() 会把所有 key 打散，不利于一次聚合 |
| count(distinct) | 先确认计划；必要时改成 group by 子查询再 count/sum | 两阶段 distinct 不一定倾斜；改写要注意 null 和重复值 |
| Window TopN | 两阶段 ROW_NUMBER：先按 key+随机分片取 TopN，再全局取 TopN | 多一次聚合，SQL 更复杂 |
| 动态分区写倾斜 | 按分区键 distribute/repartition，控制并发 | 避免 n×m 小文件；调整分区策略会影响下游 |
| map 端倾斜 | 合并小文件、改用可切分格式、repartition | 改格式/合并会影响上游写入链路 |
| filter/flatMap 后不均 | repartition | 非 shuffle 算子也可能造成分区不均 |
| 并行度不足 | 提高 shuffle 分区数 | 只能缓解，不能拆分单个 hot key |

## 验证 SQL

```sql
-- 热点 key
SELECT key, count(*) AS cnt
FROM table_name
GROUP BY key
ORDER BY cnt DESC
LIMIT 20;

-- 空值
SELECT count(*) FROM table_name WHERE key IS NULL;

-- 超过阈值的 hot key
SELECT key, count(*) AS cnt
FROM table_name
GROUP BY key
HAVING count(*) > 1000000;
```
