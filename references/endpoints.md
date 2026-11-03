# Spark UI REST 端点

## URL 到 API base

| 用户给地址 | API base |
|---|---|
| `http://host:4040` | `http://host:4040/api/v1` |
| `http://host:18080/history/application_1_2/` | `http://host:18080/api/v1` |
| `http://host:8088/proxy/application_1_2` | `http://host:8088/proxy/application_1_2/api/v1` |
| `http://host:4040/api/v1` | 原样使用 |

`application_1_2` 从路径中用正则 `application_\d+_\d+` 提取。

## 常用端点

| 端点 | 用途 |
|---|---|
| `GET /applications` | 应用列表；运行中的 4040 通常返回单个应用 |
| `GET /applications/{app}/sql` | SQL 执行列表 |
| `GET /applications/{app}/sql/{id}` | 单条 SQL 计划 + 节点 metrics |
| `GET /applications/{app}/jobs` | Job 列表 |
| `GET /applications/{app}/jobs/{jobId}` | 单个 Job，含 `stageIds` |
| `GET /applications/{app}/stages` | Stage 列表 |
| `GET /applications/{app}/stages/{stageId}/{attemptId}` | Stage 详情 |
| `GET .../stages/{stageId}/{attemptId}/taskSummary?quantiles=0,0.25,0.5,0.75,0.9,0.95,0.99` | 分位数 |
| `GET .../stages/{stageId}/{attemptId}/taskList?offset=0&length=1000` | 逐 task 指标 |
| `GET /applications/{app}/executors` | 当前 executor（动态分配可能只剩 driver） |
| `GET /applications/{app}/environment` | Spark 配置 |

## 分页

`/sql` 和 `taskList` 都支持 `offset` / `length`。

- `/sql` 默认从 offset=0 返回少量记录（常见 20 条），**最新的 SQL 往往不在默认列表里**；
- 定位最新执行时用 `?offset=N&length=100`，或直接按 Execution ID 请求 `/sql/{id}`；
- `taskList` 用 `length=1000` 循环，直到返回数量小于 length。

## 鉴权

- Cookie：`Cookie: name=value`
- Bearer / 自定义头：`--header "X-Token: ..."`
- Basic Auth：`--user user:password`
- Kerberos / SPNEGO：用 `curl --negotiate -u :`（脚本的 `--kerberos`）
- 自签证书：`--insecure`，仅限内网

## 版本差异

- Spark 2.4：`/sql` 有，但节点 metrics 很少；task 指标字段名与 3.x 有差异。
- Spark 3.x：SQL 节点 metrics 完整；AQE Final Plan 可在 `/sql/{id}` 的 `planDescription` 或驱动日志里看到。
- REST 时间通常为 `2026-11-02T08:06:50.516GMT`；日志时间是本地时区，注意换算。

## Stage attempts

`/stages` 对同一个 `stageId` 可能返回多个 attempt：

- 唯一键是 `(stageId, attemptId)`，不是 `stageId`；
- 失败重试时会有 `attemptId=0,1,...`；
- 取 task list 必须带 attempt：
  `/stages/{stageId}/{attemptId}/taskList`
- 分析时要保留所有 attempt，不能用一个字典按 `stageId` 覆盖。

## Executor 与 Job

- `/jobs/{jobId}` 返回 `stageIds`；用它可以只抓该 SQL 相关的 stage；
- `/executors` 返回当前注册的 executor；动态分配释放后可能只剩 driver；
- 历史 executor 可从 task 的 `executorId` / `host` 还原；
- `/environment` 里找 `spark.dynamicAllocation.maxExecutors`、
  `spark.executor.instances`、`spark.executor.cores`。

## 计划节点里的 stage

SQL 页面节点 metrics 的字符串可能包含：

```text
3.2 s (1.5 s, 1.7 s, 1.7 s (stage 32.0: task 32))
```

用正则 `stage (\d+)\.(\d+)` 提取 stage id，建立 stage → 计划节点映射。
