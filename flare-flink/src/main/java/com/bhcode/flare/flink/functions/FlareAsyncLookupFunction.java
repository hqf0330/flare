package com.bhcode.flare.flink.functions;

import com.bhcode.flare.core.anno.connector.AsyncLookup;
import com.bhcode.flare.flink.cache.LookupCacheManager;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Optional;
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

    protected transient LookupCacheManager<IN, OUT> cacheManager;
    protected transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        AsyncLookup anno = this.getClass().getAnnotation(AsyncLookup.class);
        if (anno == null) {
            // Try to find annotation on the outer class if this is an anonymous inner class
            anno = this.getClass().getEnclosingClass() != null ? 
                   this.getClass().getEnclosingClass().getAnnotation(AsyncLookup.class) : null;
        }

        this.cacheManager = new LookupCacheManager<>(anno);
        this.cacheManager.open();
        
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
        // 1. Try cache (including empty results)
        Optional<OUT> cached = cacheManager.getIfPresent(input);
        if (cached != null) {
            if (cached.isPresent()) {
                resultFuture.complete(Collections.singleton(cached.get()));
            } else {
                resultFuture.complete(Collections.emptyList());
            }
            return;
        }

        // 2. Async lookup
        try {
            CompletableFuture<OUT> future = lookup(input);
            if (future == null) {
                resultFuture.complete(Collections.emptyList());
                return;
            }
            
            future.thenAccept(result -> {
                // Cache the result (even if null to prevent cache penetration)
                cacheManager.put(input, result);
                
                if (result != null) {
                    resultFuture.complete(Collections.singleton(result));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            }).exceptionally(ex -> {
                log.error("Async lookup failed for input: {}", input, ex);
                resultFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            log.error("Async lookup invocation failed", e);
            resultFuture.completeExceptionally(e);
        }
    }

    /**
     * Implement this method to perform the actual asynchronous lookup.
     *
     * @param input Input record
     * @return CompletableFuture with the result
     */
    public abstract CompletableFuture<OUT> lookup(IN input);
}
