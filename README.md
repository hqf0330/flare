# 🚀 Flare - 新一代现代 Flink 开发脚手架

Flare 是一款专为 **Java 17** 和 **Flink 1.19+** 打造的高性能、极简开发脚手架。它深度参考了经典框架 `fire` 的设计精髓，并结合现代 Java 特性（Record, Lambda）进行了全方位的重构与进化。

---

## 🌟 核心特性

### 1. 极简开发模型
*   **Lambda 驱动**：维表关联、数据落库全部支持 Lambda 风格，彻底告别臃肿的匿名内部类。
*   **Record 自动映射**：利用 Java 17 Record 特性，配合内置 `DBUtils`，实现 SQL 结果到对象的零代码自动映射。
*   **注解驱动配置**：通过 `@Streaming`, `@Kafka`, `@Jdbc`, `@Redis` 等注解一键配置任务环境。

### 2. 生产级稳定性 (Production-Ready)
*   **稳健连接池**：异步 I/O 全面集成 **HikariCP** 和 **JedisPool**，支持自动重连与高并发。
*   **统一缓存管理**：内置 Caffeine 缓存，支持 **“空值保护”** 机制，有效防御缓存穿透。
*   **状态恢复保障**：算子 UID 自动化生成，确保 Checkpoint 在任务升级或扩容后依然有效。

### 3. 全链路容错与治理
*   **全局脏数据总线**：全任务关联失败数据自动收集、汇聚，支持一键分流处理。
*   **性能自动优化**：自动注册 POJO 序列化、反射元数据缓存、禁用低效 Generic Types。
*   **环境快照报告**：启动时自动打印详细的 Flink/Java/OS 环境报告，排查问题快人一步。

---

## 🛠️ 快速上手

### 1. 定义数据模型 (Record)
```java
public record OrderEvent(String orderId, String userId, Double amount) {}
public record UserInfo(String userName, String level) {}
```

### 2. 编写业务逻辑 (FlinkStreaming)
```java
@Streaming(appName = "MyEnrichJob", parallelism = 2)
@Kafka(topics = "orders", groupId = "group1")
@Jdbc(url = "jdbc:mysql://localhost:3306/db", username = "root", password = "...")
public class MyJob extends FlinkStreaming {

    @Override
    public void process() {
        // 1. 读取 Kafka
        var source = this.kafkaSourceFromConf(OrderEvent.class);

        // 2. 异步关联 MySQL (Lambda 风格 + 自动映射)
        var joined = this.asyncJdbcLookup(source, 
            "SELECT user_name as userName, level FROM t_user WHERE id = ?",
            UserInfo.class,
            (ps, order) -> ps.setString(1, order.userId()),
            (user, order) -> new EnrichedOrder(order, user)
        );

        // 3. 自动化 JDBC Sink (零代码落库)
        this.jdbcSink(joined, "t_enriched_orders");
        
        // 4. 开启脏数据自动打印
        // 配置 flare.dirty.data.print.enable = true 即可
    }
}
```

---

## 📂 项目结构

*   `flare-common`: 核心工具类、JSON 处理、DB 自动映射。
*   `flare-core`: 框架核心生命周期、注解定义。
*   `flare-connectors`: Kafka, JDBC, Redis, HBase 深度封装连接器。
*   `flare-flink`: Flink 运行时环境、异步算子包装器、指标与监控。
*   `flare-jobs`: **业务代码收容所**（Git 已忽略，专供业务开发）。

---

## 📈 性能与调优

Flare 默认开启了多项性能优化：
*   **POJO 自动注册**：无需手动调用 `registerPojoType`。
*   **反射缓存**：`DBUtils` 内部缓存构造器与字段，极大降低反射开销。
*   **配置覆盖**：`.properties` 配置文件优先级高于注解，支持线上不改代码动态调优。

---

## 🎯 愿景
让 Flink 开发像写脚本一样简单，像写微服务一样稳健。
