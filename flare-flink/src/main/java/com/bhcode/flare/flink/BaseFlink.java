package com.bhcode.flare.flink;

import com.bhcode.flare.common.anno.Config;
import com.bhcode.flare.common.util.FlareUtils;
import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.core.BaseFlare;
import com.bhcode.flare.flink.anno.Checkpoint;
import com.bhcode.flare.flink.anno.State;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.conf.FlareFlinkConf;
import com.bhcode.flare.flink.conf.FlinkAnnoManager;
import com.bhcode.flare.flink.util.FlinkSingletonFactory;
import com.bhcode.flare.flink.util.FlinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

@Slf4j
public abstract class BaseFlink extends BaseFlare {
    
    protected Configuration configuration;
    
    // TODO: Hive Catalog（可选功能，延后实现）
    // protected HiveCatalog hiveCatalog;

    /**
     * Flink 特定的初始化方法
     * 注：该方法在构造函数中自动调用，用于初始化 Flink 特定的配置
     */
    protected void initFlink() {
        // 低优先级：@Config、@Streaming/@Checkpoint（配置文件将覆盖）
        this.applyConfigAnnoProps();
        this.applyAnnoProps();

        // 高优先级：配置文件
        PropUtils.load("flink");
        PropUtils.load("flare");
        PropUtils.load(this.className);
        PropUtils.load(this.driverClass);

        if (FlareUtils.isLocalRunMode()) {
            this.loadConf();
            // TODO: 加载用户通用配置和任务配置
        }

        // 设置 Flink 相关属性
        PropUtils.setProperty(
                FlareFlinkConf.FLINK_DRIVER_CLASS_NAME, 
                this.className);
        PropUtils.setProperty(
                FlareFlinkConf.FLINK_CLIENT_SIMPLE_CLASS_NAME, 
                this.driverClass);

        // 允许通过配置覆盖 appName
        String configAppName = FlareFlinkConf.getFlinkAppName();
        if (StringUtils.isNotBlank(configAppName)) {
            this.setAppName(configAppName.trim());
        }

        FlinkSingletonFactory.getInstance().setAppName(this.appName);

        // 打印环境报告（D 计划：增强可维护性）
        this.printEnvironmentReport();

        log.debug("BaseFlink initialization completed");
    }

    /**
     * 打印环境快照报告（对标 fire）
     */
    private void printEnvironmentReport() {
        log.info("---------------------------------------------------------------------------");
        log.info("🔥 FLARE Framework Environment Report");
        log.info("---------------------------------------------------------------------------");
        log.info("Job Name: {}", this.appName);
        log.info("Driver Class: {}", this.className);
        log.info("Flink Version: {}", org.apache.flink.runtime.util.EnvironmentInformation.getVersion());
        log.info("Java Version: {}", System.getProperty("java.version"));
        log.info("OS: {} ({})", System.getProperty("os.name"), System.getProperty("os.arch"));
        
        // 打印加载的配置文件
        log.info("Loaded Configs: flink.properties, flare.properties, {}.properties", this.className);
        
        // 打印识别到的核心注解
        if (this.getClass().isAnnotationPresent(Streaming.class)) {
            Streaming s = this.getClass().getAnnotation(Streaming.class);
            log.info("Annotation [@Streaming]: parallelism={}, mode={}", s.parallelism(), s.mode());
        }
        
        log.info("---------------------------------------------------------------------------");
    }
    
    /**
     * 构造函数，自动调用 Flink 特定的初始化
     */
    protected BaseFlink() {
        super(); // 调用父类构造函数，会自动调用父类的 boot() 方法
        this.initFlink();
    }

    /**
     * 构建或合并 Configuration
     * 注：不同的子类需根据需要复写该方法
     *
     * @param conf 在 conf 基础上构建
     * @return 合并后的 Configuration 对象
     */
    public abstract Configuration buildConf(Configuration conf);

    /**
     * 获取任务的 resourceId
     *
     * @return Flink任务：JobManager/container_xxx
     */
    @Override
    protected String resourceId() {
        return FlinkUtils.getResourceId();
    }

    /**
     * SQL语法校验，如果语法错误，则返回错误堆栈
     *
     * @param sql SQL语句
     * @return 校验结果，成功返回空，失败返回异常
     */
    @Override
    public Optional<Exception> sqlValidate(String sql) {
        return FlinkUtils.sqlValidate(sql);
    }

    /**
     * SQL语法校验
     *
     * @param sql SQL语句
     * @return true：校验成功 false：校验失败
     */
    @Override
    public boolean sqlLegal(String sql) {
        return FlinkUtils.sqlLegal(sql);
    }

    /**
     * 生命周期方法：用于回收资源
     */
    @Override
    public void stop() {
        try {
            this.after();
        } finally {
            this.shutdown();
        }
    }

    /**
     * 生命周期方法：进行 Flare 框架的资源回收
     * 注：不允许子类覆盖
     */
    @Override
    protected final void shutdown(boolean stopGracefully, boolean inListener) {
        super.shutdown(stopGracefully, inListener);
        // TODO: 如果配置了退出，则退出JVM
        // if (FlareFrameworkConf.shutdownExit) {
        //     FlareUtils.exitError();
        // }
    }

    /**
     * 将类上的 Flink 注解配置映射到配置项
     */
    private void applyAnnoProps() {
        FlinkAnnoManager annoManager = new FlinkAnnoManager();
        Streaming streaming = this.getClass().getAnnotation(Streaming.class);
        if (streaming != null) {
            annoManager.mapStreaming(streaming);
        }
        Checkpoint checkpoint = this.getClass().getAnnotation(Checkpoint.class);
        if (checkpoint != null) {
            annoManager.mapCheckpoint(checkpoint);
        }
        State state = this.getClass().getAnnotation(State.class);
        if (state != null) {
            annoManager.mapState(state);
        }
        annoManager.applyToPropUtils();
    }

    /**
     * 将 @Config 注解的配置加载到 PropUtils（低优先级）
     */
    private void applyConfigAnnoProps() {
        Config config = this.getClass().getAnnotation(Config.class);
        if (config == null) {
            return;
        }
        if (config.files() != null && config.files().length > 0) {
            PropUtils.load(config.files());
        }
        if (config.props() != null) {
            for (String prop : config.props()) {
                applyKeyValue(prop);
            }
        }
        if (StringUtils.isNotBlank(config.value())) {
            String[] parts = config.value().split("[;\\n]");
            for (String part : parts) {
                applyKeyValue(part);
            }
        }
    }

    @Override
    public void counter(String name, long count) {
        if (StringUtils.isBlank(name)) return;
        // 注意：此处的 counter 在分布式环境下仅在当前 JVM（TaskManager）生效
        // 建议在算子内部使用 MetricUtils.counter(getRuntimeContext(), name, count)
        log.debug("Local JVM Metric Counter [{}]: +{}", name, count);
        FlinkSingletonFactory.getInstance().updateMetric(name, count);
    }

    private void applyKeyValue(String text) {
        if (StringUtils.isBlank(text)) {
            return;
        }
        String[] kv = text.split("=", 2);
        if (kv.length != 2) {
            return;
        }
        String key = kv[0].trim();
        String value = kv[1].trim();
        if (StringUtils.isNotBlank(key)) {
            PropUtils.setProperty(key, value);
        }
    }
}
