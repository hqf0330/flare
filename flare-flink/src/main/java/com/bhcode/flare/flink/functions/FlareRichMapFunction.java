package com.bhcode.flare.flink.functions;

import com.bhcode.flare.connector.jdbc.JdbcConnector;
import com.bhcode.flare.connector.redis.RedisConnector;
import com.bhcode.flare.core.anno.connector.AsyncLookup;
import com.bhcode.flare.flink.cache.LookupCacheManager;
import com.bhcode.flare.flink.util.MetricUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Base MapFunction with built-in distributed metrics and JDBC lookup support.
 */
public abstract class FlareRichMapFunction<IN, OUT> extends RichMapFunction<IN, OUT> {

    protected transient LookupCacheManager<String, Object> cacheManager;

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
}
