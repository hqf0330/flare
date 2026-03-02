package com.bhcode.flare.flink.functions;

import com.bhcode.flare.connector.jdbc.JdbcConnector;
import com.bhcode.flare.connector.redis.RedisConnector;
import com.bhcode.flare.core.anno.connector.AsyncLookup;
import com.bhcode.flare.flink.cache.LookupCacheManager;
import com.bhcode.flare.flink.conf.FlareFlinkConf;
import com.bhcode.flare.flink.util.FlareStateHelper;
import com.bhcode.flare.flink.util.MetricUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Base MapFunction with built-in distributed metrics and JDBC lookup support.
 */
public abstract class FlareRichMapFunction<IN, OUT> extends RichMapFunction<IN, OUT> {

    protected transient LookupCacheManager<String, Object> cacheManager;
    protected transient FlareStateHelper stateHelper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        AsyncLookup anno = this.getClass().getAnnotation(AsyncLookup.class);
        if (anno == null) {
            anno = this.getClass().getEnclosingClass() != null ? 
                   this.getClass().getEnclosingClass().getAnnotation(AsyncLookup.class) : null;
        }
        this.cacheManager = new LookupCacheManager<>(anno);
        this.cacheManager.open();
        this.stateHelper = new FlareStateHelper(
                getRuntimeContext(),
                FlareStateHelper.defaultTtlConfig(FlareFlinkConf.getFlinkStateTtlDays())
        );
    }

    /**
     * Increment a distributed counter.
     */
    protected void counter(String name, long count) {
        MetricUtils.counter(getRuntimeContext(), name, count);
    }

    protected void counter(String name) {
        this.counter(name, 1L);
    }

    // JDBC Synchronous Lookup Helpers (aligned with fire)

    protected <T> List<T> queryList(String sql, Class<T> clazz, Object... params) {
        return this.queryList(1, sql, clazz, params);
    }

    @SuppressWarnings("unchecked")
    protected <T> List<T> queryList(int keyNum, String sql, Class<T> clazz, Object... params) {
        String cacheKey = keyNum + "_list_" + sql + "_" + java.util.Arrays.toString(params);
        return (List<T>) cacheManager.get(cacheKey, k -> JdbcConnector.queryList(keyNum, sql, clazz, params));
    }

    protected <T> T queryOne(String sql, Class<T> clazz, Object... params) {
        return this.queryOne(1, sql, clazz, params);
    }

    @SuppressWarnings("unchecked")
    protected <T> T queryOne(int keyNum, String sql, Class<T> clazz, Object... params) {
        String cacheKey = keyNum + "_one_" + sql + "_" + java.util.Arrays.toString(params);
        return (T) cacheManager.get(cacheKey, k -> JdbcConnector.queryOne(keyNum, sql, clazz, params));
    }

    /**
     * Redis 同步查询
     */
    protected String redisGet(String key) {
        return this.redisGet(1, key);
    }

    protected String redisGet(int keyNum, String key) {
        return (String) cacheManager.get("redis_" + keyNum + "_" + key, k -> {
            try (Jedis jedis = RedisConnector.getJedis(keyNum)) {
                return jedis.get(key);
            }
        });
    }

    protected int executeUpdate(String sql, Object... params) {
        return JdbcConnector.executeUpdate(1, sql, params);
    }

    protected int executeUpdate(int keyNum, String sql, Object... params) {
        return JdbcConnector.executeUpdate(keyNum, sql, params);
    }

    // Flink State Helpers (TTL + cache)

    protected <T> ValueState<T> valueState(String name, Class<T> valueClass) {
        return this.requireStateHelper().valueState(name, valueClass);
    }

    protected <T> ValueState<T> valueState(String name, Class<T> valueClass, StateTtlConfig ttlConfig) {
        return this.requireStateHelper().valueState(name, valueClass, ttlConfig);
    }

    protected <T> ListState<T> listState(String name, Class<T> elementClass) {
        return this.requireStateHelper().listState(name, elementClass);
    }

    protected <T> ListState<T> listState(String name, Class<T> elementClass, StateTtlConfig ttlConfig) {
        return this.requireStateHelper().listState(name, elementClass, ttlConfig);
    }

    protected <K, V> MapState<K, V> mapState(String name, Class<K> keyClass, Class<V> valueClass) {
        return this.requireStateHelper().mapState(name, keyClass, valueClass);
    }

    protected <K, V> MapState<K, V> mapState(
            String name,
            Class<K> keyClass,
            Class<V> valueClass,
            StateTtlConfig ttlConfig) {
        return this.requireStateHelper().mapState(name, keyClass, valueClass, ttlConfig);
    }

    protected <T> ReducingState<T> reducingState(String name, Class<T> valueClass, ReduceFunction<T> reduceFunction) {
        return this.requireStateHelper().reducingState(name, valueClass, reduceFunction);
    }

    protected <T> ReducingState<T> reducingState(
            String name,
            Class<T> valueClass,
            ReduceFunction<T> reduceFunction,
            StateTtlConfig ttlConfig) {
        return this.requireStateHelper().reducingState(name, valueClass, reduceFunction, ttlConfig);
    }

    protected <IN2, ACC, OUT2> AggregatingState<IN2, OUT2> aggregatingState(
            String name,
            AggregateFunction<IN2, ACC, OUT2> aggregateFunction,
            Class<ACC> accumulatorClass) {
        return this.requireStateHelper().aggregatingState(name, aggregateFunction, accumulatorClass);
    }

    protected <IN2, ACC, OUT2> AggregatingState<IN2, OUT2> aggregatingState(
            String name,
            AggregateFunction<IN2, ACC, OUT2> aggregateFunction,
            Class<ACC> accumulatorClass,
            StateTtlConfig ttlConfig) {
        return this.requireStateHelper().aggregatingState(name, aggregateFunction, accumulatorClass, ttlConfig);
    }

    private FlareStateHelper requireStateHelper() {
        if (this.stateHelper == null) {
            this.stateHelper = new FlareStateHelper(
                    getRuntimeContext(),
                    FlareStateHelper.defaultTtlConfig(FlareFlinkConf.getFlinkStateTtlDays())
            );
        }
        return this.stateHelper;
    }
}
