---
name: spark-sql-diagnosis
description: 分析 Apache Spark / Spark SQL 任务执行情况。根据 SQL 原文、EXPLAIN FORMATTED、Kyuubi/Spark Driver 运行日志和 Spark UI 地址，定位慢查询瓶颈、数据倾斜、spill、GC、调度/冷启动、join 膨胀和配置问题。触发词：Spark UI、SparkUI、Spark SQL 分析、SQL 慢、数据倾斜、stage 慢、task 长尾、Spark 任务诊断。
allowed-tools: Bash, Read, Write, Glob, Grep
---

# Spark SQL 诊断

## 何时使用

用户提供以下任意组合时使用：

- 一个 Spark UI / History Server / YARN proxy 地址；
- 一条 Spark SQL 和它的 `EXPLAIN FORMATTED`；
- Kyuubi / Spark Driver 运行日志；
- `/api/v1/...` 导出的 JSON，或 event log。

目标不是“看几个指标”，而是回答：

1. 这次查询慢在哪一层：扫描、shuffle、join、聚合、spill、GC、调度、冷启动；
2. 有没有数据倾斜，倾斜在哪个 stage / task / SQL 片段；
3. 哪些是数据问题，哪些是资源或配置问题；
4. 下一步应该改 SQL、改配置，还是补数据。

## 输入优先级

| 输入 | 能做什么 |
|---|---|
| UI 地址 + Execution ID/提交时间 + SQL | 最完整，task 级诊断 |
| 运行日志（含 app ID 和 UI 地址）+ SQL | 先解析日志，再访问 UI；UI 不可用时降级为 stage 级 |
| EXPLAIN FORMATTED | 只能做计划结构和 SQL 片段映射 |
| event log / History Server | UI 过期后的兜底 |

日志里通常已经包含：

```
application ID: application_xxx
application web UI: http://host:8088/proxy/application_xxx
```

所以不需要用户再单独贴一遍 UI 地址，但必须实际探测地址是否可访问。

## 工作流

### 1. 收集并解析输入

- 从日志提取 app ID、UI 地址、operation ID、stage/job 时间线、Final Plan；
- 从 SQL 原文提取表名、join 条件、group by / distinct / window / order by；
- 从 `EXPLAIN FORMATTED` 提取算子、输入输出列、join 条件、聚合 key。

### 2. 定位实际执行

- 优先用 Execution ID；
- 否则按 SQL 文本 + 提交时间在 `/sql` 列表中匹配；
- `/sql` 默认只返回从 offset=0 开始的少量记录，必须分页或按 ID 探测；
- 多条候选时，选择时间最近且描述匹配的一条；仍不唯一就问用户。

### 3. 拉取运行数据

运行脚本（推荐）。下面的 `scripts/` 相对于本 skill 目录；如果在其他目录执行，请先 `cd` 到 skill 目录，或使用绝对路径。

```bash
python scripts/spark_diagnose.py \
  --url "http://host:8088/proxy/application_xxx" \
  --exec-id 94 \
  --sql-file query.sql \
  --explain explain.txt \
  --log run.log \
  --out report.md \
  --json report.json
```

如果不确定 Execution ID：

```bash
python scripts/spark_diagnose.py \
  --url "http://host:18080/history/application_xxx/" \
  --sql-file query.sql \
  --exec-time "2026-11-02T08:06:50Z" \
  --out report.md
```

鉴权参数：

```bash
--cookie "name=value; name2=value2"
--header "X-Token: ..."
--user "user:password"
--kerberos          # 使用 curl --negotiate
--insecure          # 跳过 TLS 校验，仅限内网自签证书
```

只读原则：脚本只调用 GET，不执行 SQL，不写集群。

### 4. 解读指标

脚本会输出结构化 JSON 和 Markdown。重点看：

- **Stage 时间线**：墙钟 vs `executorRunTime` vs task 启动延迟；
- **task 分布**：duration、records、shuffle bytes 的 max/median/p95；
- **真倾斜**：一个 task 的 records 和 duration 同时显著高于中位数；
- **假长尾**：task 记录数正常，但 `executorDeserializeTime` 很高，通常是冷 executor；
- **spill**：`memoryBytesSpilled` / `diskBytesSpilled`，以及计划节点的 `spill size`；
- **计划节点**：Scan、Exchange、Join、Aggregate 的 output rows / shuffle bytes / duration；
- **配置**：AQE、skewJoin 阈值、shuffle 分区数、广播阈值、动态分配。

### 5. 写最终报告

脚本给的是证据和候选结论。最终回复必须结合 SQL 语义补充：

- 算子 → SQL 片段映射；
- 热点 key 的业务含义（如果能从 SQL/建表逻辑推断）；
- 建议的优先级和预期收益；
- 明确标注“需要外部验证”的部分。

报告结构：

1. 结论先行（按影响排序）；
2. 执行概览与时间线；
3. 倾斜/长尾证据表；
4. SQL 计划映射；
5. 配置与风险；
6. 建议；
7. 证据来源与局限；
8. 仍需外部验证的问题。

## 关键判断规则

这些规则必须遵守，避免最常见的误判：

1. **倾斜看 records 和 duration，不能只看 shuffle bytes。**
   高度可压缩的热点 key（例如大量 `item_id=1`）可能只占几 MB，但记录数占 60%。

2. **区分真倾斜和冷启动。**
   慢 task 如果记录数正常、`executorDeserializeTime / duration` 很高，是冷 executor，不是数据倾斜。

3. **partial aggregation 会掩盖 GROUP BY 倾斜。**
   计划里有 `partial_count` / `partial_sum` 时，热点 key 会在 shuffle 前被压成少量行；要检查是不是聚合写法本身规避了倾斜。

4. **`count(DISTINCT)` 是两阶段去重。**
   第一次 shuffle 的 key 通常包含 distinct 字段（如 `(item_id, buyer_id)`），热点会被摊开；不要按普通 GROUP BY 判断。

5. **AQE skew join 是 size-based。**
   判断依据是压缩后的分区大小与 `skewedPartitionFactor` / `skewedPartitionThresholdInBytes`；可压缩热点可能低于阈值，不会被 split。

6. **增加 shuffle 分区数不能稀释单个 hot key。**
   单个 key 永远进一个分区；分区越多，其他分区越小，max/median 反而越大。

7. **Stage 墙钟远大于 task 运行时间时，先查调度。**
   看 `launchTime - submissionTime`、动态分配 `minExecutors`、YARN 队列。

8. **join 输出行数明显大于输入行数时，才怀疑膨胀。**
   输出行数等于事实表行数时，问题通常是分布，不是重复 key。

## 边界

- 看不到真实数据内容；热点 key 的具体值需要用户查数据；
- YARN 队列排队、容器分配慢的具体原因需要 RM/Kyuubi 日志；
- UDF/自定义代码问题需要代码和 executor 日志；
- 没有 UI/event log 时，普通 driver 日志只能做 stage 级诊断；
- 不执行 SQL、不修改数据、不打印 cookie/token。

## 参考

- `references/endpoints.md`：REST 端点与分页；
- `references/metrics.md`：字段差异与单位换算；
- `references/heuristics.md`：阈值、计划和倾斜判定。
