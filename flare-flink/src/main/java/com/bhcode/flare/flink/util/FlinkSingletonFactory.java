package com.bhcode.flare.flink.util;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class FlinkSingletonFactory {

    private static volatile FlinkSingletonFactory instance;
    
    private StreamExecutionEnvironment streamEnv;
    private TableEnvironment tableEnv;
    private String appName;
    private final Map<String, Long> metrics = new ConcurrentHashMap<>();

    private FlinkSingletonFactory() {
        // Private constructor to prevent instantiation
    }

    /**
     * 获取单例实例
     *
     * @return FlinkSingletonFactory 实例
     */
    public static FlinkSingletonFactory getInstance() {
        if (instance == null) {
            synchronized (FlinkSingletonFactory.class) {
                if (instance == null) {
                    instance = new FlinkSingletonFactory();
                }
            }
        }
        return instance;
    }

    /**
     * 更新指标值
     * 
     * @param name 指标名称
     * @param count 增加数值
     */
    public void updateMetric(String name, long count) {
        if (name != null) {
            this.metrics.merge(name, count, Long::sum);
        }
    }

    /**
     * 设置 StreamExecutionEnvironment 实例
     *
     * @param env StreamExecutionEnvironment 实例
     * @return this
     */
    public FlinkSingletonFactory setStreamEnv(StreamExecutionEnvironment env) {
        if (env != null && this.streamEnv == null) {
            this.streamEnv = env;
        }
        return this;
    }

    /**
     * 设置 TableEnvironment 实例
     *
     * @param tableEnv TableEnvironment 实例
     * @return this
     */
    public FlinkSingletonFactory setTableEnv(TableEnvironment tableEnv) {
        if (tableEnv != null && this.tableEnv == null) {
            this.tableEnv = tableEnv;
        }
        return this;
    }

    /**
     * 设置应用名称
     *
     * @param appName 应用名称
     * @return this
     */
    public FlinkSingletonFactory setAppName(String appName) {
        this.appName = appName;
        return this;
    }

    /**
     * 获取 StreamTableEnvironment 实例
     *
     * @return StreamTableEnvironment 实例
     */
    public StreamTableEnvironment getStreamTableEnv() {
        if (this.tableEnv instanceof StreamTableEnvironment) {
            return (StreamTableEnvironment) this.tableEnv;
        }
        return null;
    }
}
