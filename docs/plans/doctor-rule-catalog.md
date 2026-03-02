# Flare Doctor Rule Catalog

本文档描述 `flare-doctor` 当前内置规则，便于业务开发与 CI 集成统一处理。

## Blocking Rules (`ERROR`)

| Code | Rule | Trigger | Suggestion |
|---|---|---|---|
| `DR-000` | Job class null | 输入 job class 为空 | 传入有效的全限定类名 |
| `DR-001` | Missing `@Streaming` | 任务类未标注 `@Streaming` | 在任务类上添加 `@Streaming` |
| `DR-011` | Duplicate connector `keyNum` | 同一类连接器注解 `keyNum` 重复 | 调整为唯一 `keyNum` |
| `DR-101` | Kafka brokers missing | `@Kafka.brokers` 为空 | 配置 `@Kafka(brokers=...)` |
| `DR-102` | Kafka topics missing | `@Kafka.topics` 为空 | 配置 `@Kafka(topics=...)` |
| `DR-103` | Kafka groupId missing | `@Kafka.groupId` 为空 | 配置 `@Kafka(groupId=...)` |
| `DR-201` | JDBC url missing | `@Jdbc.url` 为空 | 配置 `@Jdbc(url=...)` |
| `DR-202` | JDBC username missing | `@Jdbc.username` 为空 | 配置 `@Jdbc(username=...)` |
| `DR-221` | Redis host missing | `@Redis.host` 为空 | 配置 `@Redis(host=...)` |
| `DR-222` | Redis port invalid | `@Redis.port <= 0` | 配置 `@Redis(port>0)` |

## Non-Blocking Rules (`WARN`)

| Code | Rule | Trigger | Suggestion |
|---|---|---|---|
| `DR-203` | JDBC password empty | `@Jdbc.password` 为空 | 配置密码或确认免密策略 |
| `DR-301` | Checkpoint disabled/invalid | `@Streaming.interval/value <= 0` | 设置 `interval>0` 或 `value>0` |
| `DR-311` | Parallelism invalid | `@Streaming.parallelism <= 0` | 设置 `parallelism>0` |
| `DR-321` | Auto start disabled | `@Streaming.autoStart=false` | 保持自动启动或补充手动执行说明 |
| `DR-331` | Deprecated autoStart key | `@Config` 中存在 `flink.job.autoStart` | 改为 `flink.job.auto.start` |
| `DR-332` | Runtime REST no token | `flare.runtime.rest.enable=true` 但 token 为空 | 配置 `flare.runtime.rest.token` |
| `DR-333` | Runtime scheduler pool invalid | `flare.runtime.schedule.pool.size <= 0` | 设为正整数 |

## Exit Code Contract

- `0`: 无阻断错误（允许存在 `WARN`）。
- `1`: 存在至少一个 `ERROR`。
- `2`: 参数错误、类加载失败、运行异常。
