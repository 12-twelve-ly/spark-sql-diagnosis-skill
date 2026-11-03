# 指标字段与单位

## Task 级

`/taskList` 每个 task 的主要字段：

```json
{
  "taskId": 67,
  "index": 29,
  "attempt": 0,
  "executorId": "17",
  "host": "worker-1",
  "status": "SUCCESS",
  "taskLocality": "NODE_LOCAL",
  "launchTime": "2026-11-02T08:07:04.123GMT",
  "duration": 3136,
  "taskMetrics": {
    "executorRunTime": 3127,
    "executorDeserializeTime": 4,
    "jvmGcTime": 132,
    "memoryBytesSpilled": 0,
    "diskBytesSpilled": 0,
    "peakExecutionMemory": 234881024,
    "inputMetrics": {"bytesRead": 0, "recordsRead": 0},
    "shuffleReadMetrics": {
      "remoteBytesRead": 2043195,
      "localBytesRead": 2034545,
      "recordsRead": 12275975,
      "fetchWaitTime": 0
    },
    "shuffleWriteMetrics": {
      "bytesWritten": 0,
      "recordsWritten": 0
    }
  }
}
```

要点：

- 倾斜看 `shuffleReadMetrics.recordsRead` 和 `duration`；
- `remoteBytesRead + localBytesRead` 才是总 shuffle 读；
- `executorDeserializeTime` 高、records 正常，通常是冷 executor；
- 字段名在不同版本可能是 `taskMetrics` / `metrics`，records 可能是 `recordsRead` / `totalRecordsRead`。

## Stage 级

`/stages/{id}/{attempt}` 主要字段：

- `numTasks`、`submissionTime`、`completionTime`
- `executorRunTime`（所有 task 运行时间之和，不是墙钟）
- `jvmGcTime`
- `inputBytes` / `inputRecords`
- `shuffleReadBytes` / `shuffleReadRecords`
- `shuffleWriteBytes` / `shuffleWriteRecords`
- `memoryBytesSpilled` / `diskBytesSpilled`
- `peakExecutionMemory`

**墙钟 = completionTime - submissionTime**。墙钟远大于最长 task，说明在等资源或调度。

## SQL 节点 metrics

`/sql/{id}` 的 `nodes[]` 每个节点有 `metrics[]`，每项是 `{name, value}`。常见名称：

| name | 含义 |
|---|---|
| `number of output rows` | 输出行数 |
| `number of files read` | 扫描文件数 |
| `size of files read` | 文件总大小 |
| `scan time` | 扫描时间 |
| `shuffle bytes written` | shuffle 写字节 |
| `shuffle records written` | shuffle 写记录数 |
| `number of partitions` | shuffle 分区数 |
| `data size` | 分区数据大小 |
| `spill size` | spill 大小 |
| `duration` | WholeStageCodegen 时长 |
| `time in aggregation build` | 聚合构建时间 |
| `peak memory` | 峰值内存 |

值可能是：

- `"20,000,000"`；
- `"3.8 MiB"`；
- `"417 ms"`；
- `"total (min, med, max (stageId: taskId))\n3.2 s (1.5 s, 1.7 s, 1.7 s (32.0: task 32))"`。

解析时取第一个带单位的数值；时间统一换算成毫秒，内存统一换算成字节。

## 单位

- 1 KiB = 1024 B，1 MiB = 1024² B，1 GiB = 1024³ B；
- 1 s = 1000 ms；
- `max/median` 用中位数，不用平均值，避免被单个热点拉偏。

## 时间

- Spark REST：`2026-11-02T08:06:50.516GMT`；
- Kyuubi 日志：`2026-11-02 16:06:50.434`，通常是本地时区（本项目常见 UTC+8）；
- 不要把日志本地时间直接当 GMT 去匹配 `/sql` 的 `submissionTime`。

## Executor 与调度

`/executors` 主要字段：

- `id`、`hostPort`
- `totalCores`、`maxTasks`
- `activeTasks`、`completedTasks`、`failedTasks`
- `totalDuration`、`totalGCTime`、`memoryUsed`、`maxMemory`

注意：开了动态分配后，空闲 executor 可能已经被释放，`/executors` 只剩 driver；
此时用 task 的 `executorId` / `host` 还原任务实际跑在哪些 executor 上。

task 的 `launchTime - stage.submissionTime` 是启动延迟；
stage 墙钟减去最长 task 是调度空档。

## Stage attempt

`/stages` 会返回同一 `stageId` 的多个 attempt：

- `stageId` + `attemptId` 才是唯一键；
- `status` 可能是 `COMPLETE`、`FAILED`、`KILLED`；
- `numFailedTasks`、`numKilledTasks` 用于识别重试；
- task list 要按 `(stageId, attemptId)` 取，不能只按 stageId 覆盖。

## 其他慢因字段

- `fetchWaitTime`：等待 shuffle 数据的时间；
- `remoteReqsDuration`：远程 block 请求耗时；
- `schedulerDelay`：task 调度延迟；
- `executorDeserializeTime`：反序列化/类加载，冷 executor 常见；
- `memoryBytesSpilled` / `diskBytesSpilled`：spill；
- `jvmGcTime`：GC 时间。

## 计划节点的 stage 信息

SQL 页面 metrics 的字符串里经常带：

```text
3.2 s (1.5 s, 1.7 s, 1.7 s (stage 32.0: task 32))
```

用正则 `stage (\d+)\.(\d+)` 可以提取 stage id，建立
`stage → 计划节点` 的映射。多个 stage 可能映射到同一个 join/aggregate。
