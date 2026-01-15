package com.bhcode.flare.core;

import com.bhcode.flare.common.enums.JobType;
import com.bhcode.flare.common.lineage.LineageManager;
import com.bhcode.flare.common.util.ExceptionBus;
import com.bhcode.flare.common.util.FlareUtils;
import com.bhcode.flare.common.util.ParameterTool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Getter
@Setter
public abstract class BaseFlare {

    // 任务启动时间戳
    protected final long launchTime = System.currentTimeMillis();

    /**
     * 设置 MDC 变量
     */
    protected void setMdc() {
        MDC.put("appName", this.appName);
        MDC.put("driverClass", this.driverClass);
    }

    // web ui地址
    protected String webUI;
    protected String applicationId;

    // main方法参数
    protected String[] args;

    // 当前任务的类型标识
    // protected final JobType jobType = JobType.UNDEFINED; 移除此字段，改用 getJobType()

    // 用于子类的锁状态判断，默认关闭状态
    protected final AtomicBoolean lock = new AtomicBoolean(false);

    // 是否已停止
    protected final AtomicBoolean isStopped = new AtomicBoolean(false);

    // 当前任务的类名（包名+类名）
    protected final String className = this.getClass().getName().replace("$", "");

    // 当前任务的类名
    protected final String driverClass = this.getClass().getSimpleName().replace("$", "");

    // 默认的任务名称为类名
    protected String appName = this.driverClass;

    public void setAppName(String appName) {
        this.appName = appName;
        MDC.put("appName", appName);
    }

    // 命令行参数工具
    protected ParameterTool parameter;

    /**
     * 构造函数，自动调用 boot() 方法
     */
    protected BaseFlare() {
        this.setAppName(this.driverClass); // 初始化 appName 并设置 MDC
        this.boot();
    }

    /**
     * 生命周期方法：初始化 Flare 框架必要的信息
     * 注：该方法会同时在 driver 端与 executor 端执行
     */
    private void boot() {
        // 注册 MDC
        MDC.put("driverClass", this.driverClass);

        // 注册 JVM 关闭钩子，确保资源回收
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (this.isRunning()) {
                log.info("JVM shutdown detected, stopping flare job...");
                this.stop();
            }
        }));

        // 显示启动画面
        FlareUtils.splash();
        log.debug("Flare framework initialized");
    }

    /**
     * 获取任务类型
     */
    public abstract JobType getJobType();

    /**
     * SQL语法校验，如果语法错误，则返回错误堆栈
     * <p>
     * 子类可以重写此方法以实现更严格的SQL校验
     * </p>
     *
     * @param sql SQL语句
     * @return 校验结果，成功返回空，失败返回异常
     */
    public Optional<Exception> sqlValidate(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return Optional.of(new IllegalArgumentException("SQL语句不能为空"));
        }
        // 简单的SQL校验：检查是否包含基本的SQL关键字
        String upperSql = sql.toUpperCase().trim();
        if (!upperSql.startsWith("SELECT") && !upperSql.startsWith("INSERT")
                && !upperSql.startsWith("UPDATE") && !upperSql.startsWith("DELETE")
                && !upperSql.startsWith("CREATE") && !upperSql.startsWith("DROP")
                && !upperSql.startsWith("ALTER") && !upperSql.startsWith("SHOW")
                && !upperSql.startsWith("DESC") && !upperSql.startsWith("EXPLAIN")
                && !upperSql.startsWith("SET") && !upperSql.startsWith("USE")) {
            return Optional.of(new IllegalArgumentException("不是有效的SQL语句"));
        }
        return Optional.empty();
    }

    /**
     * SQL语法校验
     * <p>
     * 子类可以重写此方法以实现更严格的SQL校验
     * </p>
     *
     * @param sql SQL语句
     * @return true：校验成功 false：校验失败
     */
    public boolean sqlLegal(String sql) {
        return sqlValidate(sql).isEmpty();
    }

    /**
     * 获取任务的 resourceId
     *
     * @return spark任务：driver/id  flink任务：JobManager/container_xxx
     */
    protected abstract String resourceId();

    /**
     * 在加载任务配置文件前将被加载
     */
    protected void loadConf() {
        // 加载配置文件
    }

    /**
     * 加载SQL set statement参数
     */
    protected void loadSqlConf() {
        // 加载SQL配置
    }

    /**
     * 用于将不同引擎的配置信息、累计器信息等传递到executor端或taskmanager端
     */
    protected void deployConf() {
        // 用于在分布式环境下分发配置信息
    }

    /**
     * 生命周期方法：用于在引擎初始化之前完成用户需要的动作
     * 注：该方法会在进行init之前自动被系统调用
     *
     * @param args main方法参数
     */
    public void before(String[] args) {
        // 生命周期方法，在init之前被调用
        AnnoManager.lifeCycleAnno(this, com.bhcode.flare.core.anno.connector.Before.class);
        log.debug("Before initialization");
    }

    /**
     * 生命周期方法：初始化运行信息
     *
     * @param conf 配置信息
     * @param args main方法参数
     */
    public void init(Object conf, String[] args) {
        this.parseParameter(args);
        this.before(args);
        FlareUtils.setJobType(getJobType());
        log.info("---> 完成用户资源初始化，任务类型：{} <---", getJobType().getJobTypeDesc());
        this.args = args;
        this.createContext(conf);
    }

    /**
     * 创建计算引擎运行时环境
     *
     * @param conf 配置信息
     */
    protected abstract void createContext(Object conf);

    /**
     * 在用户代码被执行之前调用
     */
    protected void preProcess() {
        // 预处理逻辑
    }

    /**
     * 生命周期方法：具体的用户开发的业务逻辑代码
     * 注：此方法会被自动调用，不需要在main中手动调用
     */
    public abstract void process();

    /**
     * 生命周期方法：依次调用process方法以及加了注解的业务逻辑处理方法
     * 注：此方法会被自动调用，不需要在main中手动调用
     */
    protected void processAll() {
        tryWithLog(() -> {
            this.loadSqlConf();
            this.preProcess();
            this.process();
            // 调用被 @Process 注解标记的方法
            AnnoManager.processAnno(this);
            // 显示血缘信息
            LineageManager.show();
        }, "业务逻辑代码执行完成", "业务逻辑代码执行失败", true);
    }

    /**
     * 生命周期方法：用于资源回收与清理，子类复写实现具体逻辑
     * 注：该方法会在进行destroy之前自动被系统调用
     */
    public void after() {
        AnnoManager.lifeCycleAnno(this, com.bhcode.flare.core.anno.connector.After.class);
        log.debug("After processing");
    }

    /**
     * 生命周期方法：用于回收资源
     */
    public abstract void stop();

    /**
     * 生命周期方法：进行 Flare 框架的资源回收
     *
     * @param stopGracefully 是否优雅停止
     * @param inListener     是否在监听器中
     */
    protected void shutdown(boolean stopGracefully, boolean inListener) {
        if (this.isStopped.compareAndSet(false, true)) {
            // TODO: ThreadUtils.shutdown(); （可选功能，延后实现）
            // TODO: Spark.stop(); // 如果使用 SparkJava（可选功能，延后实现）
            // TODO: SchedulerManager.shutdown(stopGracefully); （可选功能，延后实现）
            log.info("---> 完成 Flare 资源回收 <---");
            long elapsed = System.currentTimeMillis() - this.launchTime;
            log.info("总耗时：{} ms. The end...", elapsed);

            // TODO: 如果配置了退出，则退出JVM（可选功能，延后实现）
            // if (FlareFrameworkConf.shutdownExit) {
            //     int exitStatus = FlareUtils.isStreamingJob() ? -1 : 0;
            //     System.exit(exitStatus);
            // }
        }
    }

    /**
     * 解析命令行参数
     *
     * @param args main方法参数
     */
    protected void parseParameter(String[] args) {
        try {
            if (args != null && args.length > 0) {
                this.parameter = ParameterTool.fromArgs(args);
                log.debug("解析命令行参数，参数数量: {}", args.length);
            } else {
                this.parameter = ParameterTool.fromMap(Collections.emptyMap());
            }
        } catch (Exception e) {
            log.warn("ParameterTool 解析main方法参数失败，请注意参数的key必须以-或--开头", e);
            this.parameter = ParameterTool.fromMap(Collections.emptyMap());
        }

        // 支持通过命令行参数覆盖 appName（优先于配置）
        if (this.parameter != null) {
            String argAppName = this.parameter.get("appName");
            if (argAppName == null || argAppName.trim().isEmpty()) {
                argAppName = this.parameter.get("jobName");
            }
            if (argAppName != null && !argAppName.trim().isEmpty()) {
                this.setAppName(argAppName.trim());
                log.info("appName set from args: {}", this.appName);
            }
        }
    }

    // ========== 工具方法 ==========

    /**
     * 尝试执行代码块，如果出现异常，则记录日志
     *
     * @param block      要执行的代码块
     * @param successMsg 成功时的日志消息
     * @param errorMsg   失败时的日志消息
     * @param throwError 是否抛出异常
     */
    protected void tryWithLog(Runnable block, String successMsg, String errorMsg, boolean throwError) {
        long startTime = System.currentTimeMillis();
        try {
            block.run();
            long elapsed = System.currentTimeMillis() - startTime;
            if (successMsg != null && !successMsg.isEmpty()) {
                log.info("{}，耗时：{} ms", successMsg, elapsed);
            }
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            ExceptionBus.offAndLogError(log, errorMsg + "，耗时：" + elapsed + " ms", e);
            if (throwError) {
                throw new RuntimeException(errorMsg, e);
            }
        }
    }

    /**
     * 尝试执行代码块并返回结果，如果出现异常，则记录日志
     *
     * @param block      要执行的代码块
     * @param successMsg 成功时的日志消息
     * @param errorMsg   失败时的日志消息
     * @param <T>        返回类型
     * @return 执行结果
     */
    protected <T> T tryWithReturn(java.util.function.Supplier<T> block, String successMsg, String errorMsg) {
        long startTime = System.currentTimeMillis();
        try {
            T result = block.get();
            long elapsed = System.currentTimeMillis() - startTime;
            if (successMsg != null && !successMsg.isEmpty()) {
                log.info("{}，耗时：{} ms", successMsg, elapsed);
            }
            return result;
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            ExceptionBus.offAndLogError(log, errorMsg + "，耗时：" + elapsed + " ms", e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    /**
     * 计算从启动到现在的耗时（毫秒）
     *
     * @return 耗时（毫秒）
     */
    protected long elapsed() {
        return System.currentTimeMillis() - this.launchTime;
    }

    /**
     * 计算从指定时间到现在的耗时（毫秒）
     *
     * @param startTime 开始时间
     * @return 耗时（毫秒）
     */
    protected long elapsed(long startTime) {
        return System.currentTimeMillis() - startTime;
    }

    /**
     * 格式化耗时，转换为人类可读的格式
     *
     * @param millis 毫秒数
     * @return 格式化后的耗时字符串
     */
    protected String formatElapsed(long millis) {
        if (millis < 1000) {
            return millis + " ms";
        } else if (millis < 60000) {
            return (millis / 1000.0) + " s";
        } else if (millis < 3600000) {
            return (millis / 60000.0) + " min";
        } else {
            return (millis / 3600000.0) + " h";
        }
    }

    /**
     * 获取从启动到现在的格式化耗时
     *
     * @return 格式化后的耗时字符串
     */
    protected String getElapsedTime() {
        return formatElapsed(elapsed());
    }

    /**
     * 检查任务是否已停止
     *
     * @return true：已停止 false：未停止
     */
    public boolean isStopped() {
        return this.isStopped.get();
    }

    /**
     * 检查任务是否正在运行
     *
     * @return true：正在运行 false：已停止
     */
    public boolean isRunning() {
        return !this.isStopped.get();
    }

    /**
     * 累加器计数器（用于监控任务吞吐、异常等）
     * 
     * @param name 计数器名称
     */
    public void counter(String name) {
        this.counter(name, 1L);
    }

    /**
     * 累加器计数器
     * 
     * @param name  计数器名称
     * @param count 累加数值
     */
    public abstract void counter(String name, long count);

    /**
     * 获取锁状态
     *
     * @return true：已锁定 false：未锁定
     */
    public boolean isLocked() {
        return this.lock.get();
    }

    /**
     * 默认的 shutdown 方法（无参数）
     */
    protected void shutdown() {
        shutdown(true, false);
    }

    /**
     * 默认的 init 方法（无参数）
     */
    public void init() {
        init(null, null);
    }

    /**
     * 默认的 init 方法（仅配置）
     *
     * @param conf 配置信息
     */
    public void init(Object conf) {
        init(conf, null);
    }

    /**
     * 默认的 before 方法（无参数）
     */
    public void before() {
        before(null);
    }

}
