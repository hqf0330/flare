# Flare: 轻量级 Flink Java 开发框架

Flare 是一个专为 **Flink 1.19+** 和 **JDK 17** 深度定制的高级开发框架。它通过“注解驱动配置”和“标准化生命周期管理”，极大地简化了 Flink JAR 任务的开发成本，并内置了企业级的监控、治理与容灾能力。

## 🚀 核心特性

*   **注解驱动配置**：通过 `@Streaming`, `@Kafka`, `@Jdbc`, `@State` 等注解替代繁琐的代码配置，实现零样板代码。
*   **标准化生命周期**：定义了清晰的 `init` -> `before` -> `process` -> `after` 任务流，支持 `Step1-6` 分阶段逻辑拆分。
*   **现代 Java 适配**：深度支持 **Java 17 record**，实现 Kafka JSON 数据到 DTO 的自动反序列化。
*   **工业级容灾**：一键开启 **RocksDB** 状态后端、增量检查点（Incremental Checkpoint）以及全局状态过期（TTL）管理。
*   **全方位可观测性**：
    *   **分布式指标**：一行代码实现跨节点的分布式累加器与实时 Metrics 打点。
    *   **自动血缘**：启动即打印数据源（Source）与落地端（Sink）的拓扑关系。
    *   **日志追踪**：自动注入 `MDC` 变量（appName），支持在海量集群日志中秒级检索。
*   **生产增强**：内置 JDBC **Upsert (幂等写入)** 自动生成逻辑，解决任务重启导致的数据重复痛点。

## 🛠️ 技术栈

*   **Runtime**: JRE 17+
*   **Engine**: Apache Flink 1.19.1
*   **Build**: Maven 3.8+
*   **Frameworks**: Lombok, Jackson, SLF4J

## 📦 快速开始

### 1. 引入依赖
在你的 `pom.xml` 中引入 `flare-flink` 模块。

### 2. 开发第一个 Flare 任务
继承 `FlinkStreaming` 基类，使用注解定义环境，并在 `process` 中编写业务逻辑。

```java
@Streaming(parallelism = 2, interval = 10)
@Kafka(
    brokers = "localhost:9092", 
    topics = "user_action", 
    groupId = "flare_group",
    watermarkStrategy = "bounded"
)
@Jdbc(
    url = "jdbc:mysql://localhost:3306/db",
    sql = "INSERT INTO t_report(id, val) VALUES (?, ?)",
    upsertMode = "mysql",
    keyColumns = "id"
)
public class MyFirstTask extends FlinkStreaming {

    // 定义数据模型
    public record UserAction(Long id, String action) {}

    @Override
    public void process() {
        // 1. 自动解析 Kafka JSON 为 Record
        DataStream<UserAction> stream = this.kafkaSourceFromConf(UserAction.class);
        
        // 2. 算子 UID 管理 (确保状态恢复兼容性)
        this.uname(stream, "source_id");

        // 3. 分布式指标打点
        stream.map(new FlareRichMapFunction<UserAction, UserAction>() {
            @Override
            public UserAction map(UserAction value) {
                counter("user_login_count");
                return value;
            }
        });

        // 4. 自动攒批、自动生成 Upsert SQL 写入 MySQL
        this.jdbcSinkFromConf(stream, (ps, value) -> {
            ps.setLong(1, value.id());
            ps.setString(2, value.action());
        });
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(MyFirstTask.class, args);
    }
}
```

## 📖 注解详解

### `@Streaming`
控制 Flink 运行模式、并行度、Checkpoint 间隔等。

### `@Kafka`
配置 Kafka 连接信息。支持 `startFromTimestamp`（时间戳启动）和 `config`（底层参数透传）。

### `@Jdbc`
配置数据库连接。核心属性 `upsertMode="mysql"` 会自动将 `INSERT` 语句增强为 `ON DUPLICATE KEY UPDATE`。

### `@State`
配置状态后端。支持 `backend="rocksdb"`，并可指定 `checkpointDir` 的 HDFS 路径。

## 🛡️ 生产环境建议

1.  **状态恢复**：务必使用 `this.uname(stream, "unique_id")` 为关键算子设置 ID。
2.  **日志检索**：在日志配置文件（logback.xml）的 Pattern 中加入 `%X{appName}`。
3.  **资源回收**：框架内置了 JVM Shutdown Hook，确保在任务停止时优雅关闭数据库连接。

## 📝 开源协议
Apache License 2.0
