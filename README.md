# 🚀 Flare - 注解驱动的 Flink DataStream JAR 脚手架

Flare 是一款专为 **Java 17** 和 **Flink 1.19+** 打造的 Flink 程序化开发脚手架，核心目标是通过注解配置和统一工具层，简化 DataStream/JAR 任务的开发与落地。

---

## 🎯 项目边界（Scope）

### 当前范围（In Scope）
*   面向 **Flink DataStream JAR** 任务开发。
*   通过 `@Streaming`, `@Kafka`, `@Jdbc`, `@Redis`, `@AsyncLookup` 等注解进行配置。
*   提供常用连接器、异步维表关联、脏数据侧输出、自动化 Sink 等能力。

### 暂不纳入（Out of Scope）
*   **Flink SQL 作业开发链路**（包含 SQL 编排、UDF 自动注册等）。
*   以 SQL/Table 为主的平台化能力。
*   与 SQL 专属运行时治理深度绑定的能力。

> 默认情况下 `flink.table.env.enable=false`，即不初始化 TableEnvironment。

---

## 🌟 核心特性

### 1. 极简开发模型
*   **Lambda 驱动**：维表关联、数据落库全部支持 Lambda 风格，彻底告别臃肿的匿名内部类。
*   **Record 自动映射**：利用 Java 17 Record 特性，配合内置 `DBUtils`，实现 SQL 结果到对象的零代码自动映射。
*   **注解驱动配置**：通过 `@Streaming`, `@Kafka`, `@Jdbc`, `@Redis` 等注解一键配置 DataStream 任务环境。

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

## 🧭 Golden Path（Starter + Doctor）

### 1. 生成任务骨架
```bash
./scripts/flare-starter.sh \
  --template kafka-process-print \
  --job DemoJob \
  --out /tmp/flare-demo
```

支持模板：
* `kafka-process-print`
* `kafka-asyncJdbc-jdbcSink`

默认生成：
* `src/main/java/com/example/<Job>.java`
* `src/main/resources/flink-streaming.properties`
* `pom.xml`（可直接 `mvn package` 产出提交包）
* `run-local.sh`（本地打包+运行）
* `submit.sh`（打包后 `flink run` 提交）
* `README-run.md`

### 2. 运行预检（Doctor）
```bash
./scripts/flare-doctor.sh --job com.example.DemoJob
```

退出码约定：
* `0`：无阻断错误（可含 `WARN`）。
* `1`：存在 `ERROR` 级问题。
* `2`：参数错误/类加载失败/运行异常。

规则清单见：`docs/plans/doctor-rule-catalog.md`

### 3. 一键验证 Golden Path
```bash
bash scripts/verify-golden-path.sh
```

## 🔧 Runtime 治理（可选）

REST 控制默认关闭；内部调度默认开启（仅在存在 `@Scheduled` 方法时生效）：

```properties
flare.runtime.rest.enable=true
flare.runtime.rest.host=0.0.0.0
flare.runtime.rest.port=0
flare.runtime.rest.token=

flare.runtime.schedule.enable=true
flare.runtime.schedule.pool.size=1
```

端点：
* `POST /system/kill`：触发任务停止
* `POST /system/setConf`：动态更新 `key=value` 配置
* `POST /system/checkpoint`：更新 checkpoint 参数（并尝试热应用）
* `GET /system/datasource`：查询结构化数据源/链路快照
* `GET /system/exception`：查询并按参数清理异常总线（`clear`,`limit`）
* `GET /system/config`：查询当前生效配置快照
* `GET /system/metrics`：查询框架内累计指标快照
* `GET /system/lineage`：查询结构化 lineage 快照（source/target/operation/count）
* `GET /system/distributeSync`：查询最近一次分布式配置同步快照（module/payload/version/timestamp）
* `POST /system/collectLineage`：上报并合并外部 lineage 边（JSON 数组）

示例：
```bash
curl -X POST http://127.0.0.1:<port>/system/setConf \
  -H "Content-Type: application/json" \
  -d '{"flink.default.parallelism":"4"}'

curl -X POST http://127.0.0.1:<port>/system/collectLineage \
  -H "Content-Type: application/json" \
  -d '[{"source":"Kafka:orders","target":"Flink","operation":"SOURCE","count":3}]'
```

内部调度（轻量，无 cron）：

```java
import com.bhcode.flare.common.anno.Scheduled;

@Scheduled(fixedInterval = 60000, initialDelay = 10000)
public void refreshLocalCache() {
    // 非业务链路的周期任务（如缓存刷新、心跳上报）
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
让 Flink DataStream JAR 开发像写脚本一样简单，像写微服务一样稳健。
