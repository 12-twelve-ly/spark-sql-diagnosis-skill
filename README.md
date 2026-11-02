# spark-sql-diagnosis

只读的 Spark SQL 诊断 skill：给 SQL、`EXPLAIN FORMATTED`、Kyuubi/Spark Driver 日志和 Spark UI 地址，定位慢查询、数据倾斜、spill、冷启动和配置问题。

## 安装

把整个目录复制到用户级 skill 目录：

```bash
cp -r spark-sql-diagnosis ~/.claude/skills/
```

或在当前目录直接运行脚本。

## 快速使用

```bash
python scripts/spark_diagnose.py \
  --url "http://host:8088/proxy/application_xxx" \
  --exec-id 94 \
  --sql-file query.sql \
  --out report.md \
  --json report.json
```

从日志自动识别 app ID 和 UI 地址：

```bash
python scripts/spark_diagnose.py \
  --log run.log \
  --sql-file query.sql \
  --out report.md
```

UI 不可用时会自动降级为仅日志报告。

## 常用参数

| 参数 | 说明 |
|---|---|
| `--url` | Spark UI / History Server / YARN proxy 地址 |
| `--app-id` | application id |
| `--exec-id` | SQL Execution ID；最准确 |
| `--exec-time` | SQL 提交时间，配合 SQL 文本定位 |
| `--sql-file` / `--sql` | SQL 原文 |
| `--explain` | EXPLAIN FORMATTED 文件 |
| `--log` | Kyuubi/Spark Driver 日志 |
| `--cookie` / `--header` / `--user` / `--kerberos` | 鉴权 |
| `--insecure` | 跳过 TLS 校验，仅限内网 |
| `--out` / `--json` | 输出报告和结构化结果 |

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖：

- SortMergeJoin 单 key 倾斜：识别 44.6x records 倾斜、192MiB spill、冷启动干扰；
- `count(DISTINCT)` 两阶段去重：不误判为倾斜；
- 普通 `GROUP BY` + 冷启动：识别 8.45s task 启动延迟和 8192 分区过量；
- URL 解析、单位解析、日志解析。

## 文件

- `SKILL.md`：skill 入口和诊断流程；
- `scripts/spark_ui.py`：REST 客户端、URL/app/SQL 定位、分页；
- `scripts/analyze.py`：task/stage/plan/config 分析与 findings；
- `scripts/report.py`：中文 Markdown 报告；
- `scripts/spark_diagnose.py`：CLI、日志解析、UI 降级；
- `references/`：端点、指标字段、判定阈值；
- `tests/`：回归用例。
