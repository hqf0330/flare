# Flare 框架迭代计划 (Roadmap)

本文档旨在规划 Flare 框架的后续迭代方向，重点参考 `fire` 框架的成熟特性，补齐 Flare 在 Flink JAR 任务开发维度的生产级能力。

---

## 🎯 核心目标
将 Flare 打造为 **Java 17 + Flink 1.19** 时代下，最符合生产实战、性能最优、治理最全的 Flink 开发脚手架。

---

## 🛠️ 迭代阶段规划

### 第一阶段：性能与稳定性增强 (P0)
**目标：解决“查不动”和“容易挂”的问题。**

1.  **异步 I/O (Async I/O) 深度封装**
    *   [x] 封装 `AsyncJdbcConnector`，支持异步查询 MySQL 维表。
    *   [x] 内置 **LRU 缓存机制**（支持 Caffeine/Guava Cache），减少对外部数据库的冲击。
    *   [x] 支持注解驱动的异步关联（如 `@AsyncLookup`）。

2.  **侧输出流 (Side Output) 标准化**
    *   [x] 在 `FlinkStreaming` 中内置 `dirtyDataStream`。
    *   [x] 提供通用的脏数据处理接口，支持解析失败、业务异常数据自动分流。
    *   [x] 实现脏数据自动落地（Kafka/HDFS），确保主流程不因单条数据异常而崩溃。

3.  **序列化性能优化**
    *   [ ] 针对大流量场景，引入除 Jackson 之外的高性能序列化方案（如集成 Flink 原生 POJO 序列化优化）。
    *   [ ] 减少反射调用，提升 JSON 解析效率。

---

### 第二阶段：连接器版图扩张 (P1)
**目标：覆盖公司主流大数据生态。**

1.  **HBase 连接器增强**
    *   [x] 仿照 JDBC 实现 `@HBase` 注解驱动配置。
    *   [x] 封装 `HBaseSource` 和 `HBaseSink`，支持 `BufferedMutator` 批量高性能写入。
    *   [ ] 支持 HBase 维表的异步查询。

2.  **Redis 连接器支持**
    *   [ ] 支持 `@Redis` 注解，封装 Jedis/Lettuce 客户端。
    *   [ ] 支持实时指标快速累加（Increment）和维表缓存读取。

3.  **ClickHouse 连接器支持**
    *   [ ] 针对 OLAP 场景，封装高性能的 ClickHouse Sink（支持 HTTP/TCP 协议）。

---

### 第三阶段：质量与运维治理 (P1)
**目标：提升开发效率与任务可管控性。**

1.  **单元测试脚手架 (`FlareTestBase`)**
    *   [x] 提供集成 `TestStreamEnvironment` 的测试基类。
    *   [x] 封装 `MockSource` 和 `TestSink`，支持在不依赖外部组件的情况下编写业务逻辑测试。
    *   [x] 支持对 `counter` 指标和 `Side Output` 的断言校验。

2.  **Savepoint 自动化管理**
    *   [ ] 增强 `FlinkJobLauncher`，支持在 `stop` 时自动触发 `stopWithSavepoint`。
    *   [ ] 实现 Savepoint 路径的自动记录与启动时的自动恢复（Resume）。

3.  **运行时动态调优**
    *   [ ] 预留配置中心（如 Apollo/Nacos）接口，支持在不重启任务的情况下修改部分参数（如并行度、TTL、业务阈值）。

---

### 第四阶段：生态与高级特性 (P2)
**目标：向全能型框架进化。**

1.  **自动化 UDF 注册**
    *   [ ] 支持自动扫描指定 Package 下的类并注册为 Flink SQL UDF。

2.  **轻量级 REST 监控接口**
    *   [ ] 在任务内部集成轻量级 HTTP 服务，支持通过接口查询当前的 `metrics`、`lineage` 和 `config`。

---

## 📈 优先级排序建议
1.  **Async I/O 封装** (解决性能瓶颈)
2.  **Side Output 脏数据处理** (解决生产稳定性)
3.  **HBase 连接器** (补齐存储版图)
4.  **FlareTestBase** (提升开发质量)

---
*最后更新时间：2026-01-15*
