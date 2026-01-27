package com.bhcode.flare.flink.functions;

import com.bhcode.flare.connector.redis.RedisConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.CompletableFuture;

/**
 * Lambda-based Redis asynchronous lookup function.
 */
@Slf4j
public class LambdaAsyncRedisLookupFunction<IN, OUT> extends FlareAsyncLookupFunction<IN, OUT> {

    private final int keyNum;
    private final RedisLookupLogic<IN, OUT> logic;
    private transient JedisPool jedisPool;

    public LambdaAsyncRedisLookupFunction(int keyNum, RedisLookupLogic<IN, OUT> logic) {
        this.keyNum = keyNum;
        this.logic = logic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.jedisPool = RedisConnector.getJedisPool(keyNum);
    }

    @Override
    public CompletableFuture<OUT> lookup(IN input) {
        return CompletableFuture.supplyAsync(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                return logic.lookup(jedis, input);
            } catch (Exception e) {
                log.error("Redis async lookup error", e);
                throw new RuntimeException("Redis Async Lookup Failed", e);
            }
        }, executorService);
    }

    @FunctionalInterface
    public interface RedisLookupLogic<IN, OUT> extends java.io.Serializable {
        OUT lookup(Jedis jedis, IN input) throws Exception;
    }
}
