# Flare 框架迭代计划 (Roadmap)

本文档旨在规划 Flare 框架的后续迭代方向，重点参考 `fire` 框架的成熟特性，补齐 Flare 在 Flink JAR 任务开发维度的生产级能力。

---

## 🎯 核心目标
将 Flare 打造为 **Java 17 + Flink 1.19** 时代下，最符合生产实战、性能最优、治理最全的 Flink 开发脚手架。

---

## 🛠️ 迭代阶段规划

### 第一阶段：性能与稳定性增强 (P0) - DONE 🚀
**目标：解决“查不动”和“容易挂”的问题。**

1.  **异步 I/O (Async I/O) 深度封装**
    *   [x] 封装 `LambdaAsyncJdbcLookupFunction`，支持异步查询 MySQL 维表。
    *   [x] 内置 **LRU 缓存机制**（支持 Caffeine），具备 **“空值保护”** 能力。
    *   [x] 支持注解驱动的异步关联（如 `@AsyncLookup`）。

2.  **侧输出流 (Side Output) 标准化**
    *   [x] 在 `FlinkStreaming` 中内置 `dirtyDataStreams` 全局收集器。
    *   [x] 实现 **“全局脏数据总线”**，支持一键汇聚全任务关联失败的数据。
    *   [x] 实现脏数据自动打印开关 `flare.dirty.data.print.enable`。

3.  **序列化与反射性能优化**
    *   [x] **自动化 POJO 注册**：框架自动扫描并注册 Record/POJO 类，提升传输效率。
    *   [x] **反射元数据缓存**：在 `DBUtils` 中缓存构造器和字段，极大降低反射开销。
    *   [x] **禁用 Generic Types**：强制高性能序列化路径。

---

### 第二阶段：连接器版图扩张 (P1) - DONE 🚀
**目标：覆盖公司主流大数据生态。**

1.  **Redis 连接器深度支持**
    *   [x] 支持 `@Redis` 注解驱动配置。
    *   [x] 封装 `asyncRedisLookup`，支持 Lambda 直接操作 Jedis 实例。
    *   [x] 同步查询支持：在 `FlareRichMapFunction` 中内置 `redisGet` 并享有缓存保护。

2.  **自动化 Sink 体系**
    *   [x] **JDBC 零代码 Sink**：根据 Record 字段自动生成 UPSERT SQL 并自动绑定参数。
    *   [x] **Kafka 零代码 Sink**：支持对象自动转 JSON 写入，支持多集群路由。
    *   [x] **智能命名转换**：自动处理 Java `camelCase` 到数据库 `snake_case` 的映射。

3.  **HBase 连接器增强**
    *   [x] 仿照 JDBC 实现 `@HBase` 注解驱动配置。
    *   [x] 封装 `HBaseSource` 和 `HBaseSink`。

---

### 第三阶段：质量与运维治理 (P1) - IN PROGRESS 🏗️
**目标：提升开发效率与任务可管控性。**

1.  **单元测试脚手架 (`FlareTestBase`)**
    *   [x] 提供集成 `TestStreamEnvironment` 的测试基类。
    *   [x] 封装 `MockSource` 和 `TestSink`，支持业务逻辑 Mock 测试。
    *   [x] 示例：`BusinessLogicTest.java` 已落地。

2.  **状态恢复与可观测性**
    *   [x] **算子 UID 自动化**：自动生成稳定 UID，确保 Checkpoint 跨版本恢复。
    *   [x] **环境快照报告**：启动时自动打印 Flink/Java/OS 版本及核心配置信息。

3.  **运行时动态调优**
    *   [ ] 预留配置中心接口，支持在不重启任务的情况下修改部分参数（如并行度、TTL）。

---

### 第四阶段：生态与高级特性 (P2)
**目标：向全能型框架进化。**

1.  **自动化 UDF 注册**
    *   [ ] 支持自动扫描指定 Package 下的类并注册为 Flink SQL UDF。

2.  **轻量级 REST 监控接口**
    *   [ ] 在任务内部集成轻量级 HTTP 服务，支持查询 `metrics`、`lineage` 和 `config`。

---

## 📈 优先级记录
1.  **Async I/O 封装** (已完成)
2.  **Side Output 脏数据处理** (已完成)
3.  **自动化 Sink 体系** (已完成)
4.  **序列化性能优化** (已完成)
5.  **FlareTestBase** (已完成)

---
*最后更新时间：2026-01-27*
