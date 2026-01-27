package com.bhcode.flare.connector.redis;

import com.bhcode.flare.common.util.PropUtils;
import com.bhcode.flare.core.anno.connector.Redis;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis connector utility managing Jedis pools.
 */
@Slf4j
public class RedisConnector {

    private static final Map<Integer, JedisPool> POOLS = new ConcurrentHashMap<>();

    public static String redisPrefix(int keyNum) {
        return "flare.redis." + keyNum + ".";
    }

    /**
     * Apply Redis annotation properties to PropUtils.
     */
    public static void applyRedisAnnotation(Redis anno) {
        String prefix = redisPrefix(anno.keyNum());
        PropUtils.setProperty(prefix + "host", anno.host());
        PropUtils.setProperty(prefix + "port", String.valueOf(anno.port()));
        PropUtils.setProperty(prefix + "password", anno.password());
        PropUtils.setProperty(prefix + "database", String.valueOf(anno.database()));
        PropUtils.setProperty(prefix + "timeout", String.valueOf(anno.timeout()));
        PropUtils.setProperty(prefix + "maxTotal", String.valueOf(anno.maxTotal()));
        PropUtils.setProperty(prefix + "maxIdle", String.valueOf(anno.maxIdle()));
        PropUtils.setProperty(prefix + "minIdle", String.valueOf(anno.minIdle()));
    }

    /**
     * Get or create a JedisPool for the given keyNum.
     */
    public static JedisPool getJedisPool(int keyNum) {
        return POOLS.computeIfAbsent(keyNum, k -> {
            String prefix = redisPrefix(k);
            String host = PropUtils.getString(prefix + "host", "localhost");
            int port = PropUtils.getInt(prefix + "port", 6379);
            String password = PropUtils.getString(prefix + "password", "");
            int database = PropUtils.getInt(prefix + "database", 0);
            int timeout = PropUtils.getInt(prefix + "timeout", 2000);

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(PropUtils.getInt(prefix + "maxTotal", 8));
            config.setMaxIdle(PropUtils.getInt(prefix + "maxIdle", 8));
            config.setMinIdle(PropUtils.getInt(prefix + "minIdle", 0));

            log.info("Initializing JedisPool for keyNum={}, host={}, port={}, database={}", k, host, port, database);
            
            if (password != null && !password.isEmpty()) {
                return new JedisPool(config, host, port, timeout, password, database);
            } else {
                return new JedisPool(config, host, port, timeout, null, database);
            }
        });
    }

    /**
     * Get a Jedis instance from the pool.
     */
    public static Jedis getJedis(int keyNum) {
        return getJedisPool(keyNum).getResource();
    }
}
