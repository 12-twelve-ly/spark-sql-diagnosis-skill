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
