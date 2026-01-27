package com.bhcode.flare.flink.cache;

import com.bhcode.flare.core.anno.connector.AsyncLookup;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Unified cache manager for both sync and async lookups.
 * Handles Caffeine configuration, null value caching (Optional), and metrics.
 */
@Slf4j
public class LookupCacheManager<K, V> implements Serializable {

    private final long cacheSize;
    private final long cacheExpire;
    private final TimeUnit unit;
    
    private transient Cache<K, Optional<V>> cache;

    public LookupCacheManager(AsyncLookup anno) {
        if (anno != null) {
            this.cacheSize = anno.cacheSize();
            this.cacheExpire = anno.cacheExpire();
            this.unit = anno.cacheUnit();
        } else {
            // Default settings if no annotation present
            this.cacheSize = 10000;
            this.cacheExpire = 60;
            this.unit = TimeUnit.SECONDS;
        }
    }

    /**
     * Initialize the actual Caffeine cache (called in open() of functions).
     */
    public void open() {
        if (cacheSize > 0) {
            this.cache = Caffeine.newBuilder()
                    .maximumSize(cacheSize)
                    .expireAfterWrite(cacheExpire, unit)
                    .build();
            log.info("Lookup cache initialized: size={}, expire={} {}", cacheSize, cacheExpire, unit);
        }
    }

    /**
     * Get from cache or load from data source.
     */
    public V get(K key, Function<K, V> loader) {
        if (cache == null) return loader.apply(key);

        Optional<V> cached = cache.getIfPresent(key);
        if (cached != null) {
            return cached.orElse(null);
        }

        V value = loader.apply(key);
        cache.put(key, Optional.ofNullable(value));
        return value;
    }

    /**
     * For async usage: get from cache or return null if miss.
     */
    public Optional<V> getIfPresent(K key) {
        return cache != null ? cache.getIfPresent(key) : null;
    }

    /**
     * For async usage: put into cache.
     */
    public void put(K key, V value) {
        if (cache != null) {
            cache.put(key, Optional.ofNullable(value));
        }
    }
}
