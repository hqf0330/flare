package com.bhcode.flare.flink.functions;

import com.bhcode.flare.core.anno.connector.AsyncLookup;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Base class for asynchronous lookup with built-in Caffeine cache.
 *
 * @param <IN>  Input type
 * @param <OUT> Output type
 */
@Slf4j
public abstract class FlareAsyncLookupFunction<IN, OUT> extends RichAsyncFunction<IN, OUT> {

    protected transient Cache<IN, OUT> cache;
    protected transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        AsyncLookup anno = this.getClass().getAnnotation(AsyncLookup.class);
        long cacheSize = 10000;
        long cacheExpire = 60;
        java.util.concurrent.TimeUnit unit = java.util.concurrent.TimeUnit.SECONDS;

        if (anno != null) {
            cacheSize = anno.cacheSize();
            cacheExpire = anno.cacheExpire();
            unit = anno.cacheUnit();
        }

        this.cache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cacheExpire, unit)
                .build();
        
        // Default thread pool for async operations if needed
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.executorService != null) {
            this.executorService.shutdown();
        }
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) {
        // 1. Try cache
        OUT cached = cache.getIfPresent(input);
        if (cached != null) {
            resultFuture.complete(Collections.singleton(cached));
            return;
        }

        // 2. Async lookup
        CompletableFuture<OUT> future = lookup(input);
        future.thenAccept(result -> {
            if (result != null) {
                cache.put(input, result);
                resultFuture.complete(Collections.singleton(result));
            } else {
                resultFuture.complete(Collections.emptyList());
            }
        }).exceptionally(ex -> {
            log.error("Async lookup failed for input: {}", input, ex);
            resultFuture.completeExceptionally(ex);
            return null;
        });
    }

    /**
     * Implement this method to perform the actual asynchronous lookup.
     *
     * @param input Input record
     * @return CompletableFuture with the result
     */
    public abstract CompletableFuture<OUT> lookup(IN input);
}
