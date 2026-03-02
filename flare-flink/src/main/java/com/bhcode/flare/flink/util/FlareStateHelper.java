package com.bhcode.flare.flink.util;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * State helper for RichFunction implementations.
 * Provides typed state creation, descriptor-level TTL support and state object caching.
 */
public class FlareStateHelper {

    private static final String VALUE_PREFIX = "value";
    private static final String LIST_PREFIX = "list";
    private static final String MAP_PREFIX = "map";
    private static final String REDUCING_PREFIX = "reducing";
    private static final String AGGREGATING_PREFIX = "aggregating";

    private final RuntimeContext runtimeContext;
    private final StateTtlConfig defaultTtlConfig;
    private final ConcurrentMap<String, State> stateCache = new ConcurrentHashMap<>();

    public FlareStateHelper(RuntimeContext runtimeContext, StateTtlConfig defaultTtlConfig) {
        this.runtimeContext = Objects.requireNonNull(runtimeContext, "RuntimeContext must not be null");
        this.defaultTtlConfig = defaultTtlConfig;
    }

    /**
     * Build the default TTL config with Fire-compatible semantics.
     */
    public static StateTtlConfig defaultTtlConfig(int ttlDays) {
        int effectiveTtlDays = Math.max(1, ttlDays);
        return StateTtlConfig.newBuilder(Time.days(effectiveTtlDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
    }

    public <T> ValueState<T> valueState(String name, Class<T> valueClass) {
        return this.valueState(name, valueClass, this.defaultTtlConfig);
    }

    @SuppressWarnings("unchecked")
    public <T> ValueState<T> valueState(String name, Class<T> valueClass, StateTtlConfig ttlConfig) {
        validateName(name);
        Objects.requireNonNull(valueClass, "Value state class must not be null");
        return (ValueState<T>) this.stateCache.computeIfAbsent(cacheKey(VALUE_PREFIX, name), key -> {
            ValueStateDescriptor<T> descriptor = new ValueStateDescriptor<>(name, valueClass);
            enableTtl(descriptor, ttlConfig);
            return this.runtimeContext.getState(descriptor);
        });
    }

    public <T> ListState<T> listState(String name, Class<T> elementClass) {
        return this.listState(name, elementClass, this.defaultTtlConfig);
    }

    @SuppressWarnings("unchecked")
    public <T> ListState<T> listState(String name, Class<T> elementClass, StateTtlConfig ttlConfig) {
        validateName(name);
        Objects.requireNonNull(elementClass, "List state element class must not be null");
        return (ListState<T>) this.stateCache.computeIfAbsent(cacheKey(LIST_PREFIX, name), key -> {
            ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, elementClass);
            enableTtl(descriptor, ttlConfig);
            return this.runtimeContext.getListState(descriptor);
        });
    }

    public <K, V> MapState<K, V> mapState(String name, Class<K> keyClass, Class<V> valueClass) {
        return this.mapState(name, keyClass, valueClass, this.defaultTtlConfig);
    }

    @SuppressWarnings("unchecked")
    public <K, V> MapState<K, V> mapState(
            String name,
            Class<K> keyClass,
            Class<V> valueClass,
            StateTtlConfig ttlConfig) {
        validateName(name);
        Objects.requireNonNull(keyClass, "Map state key class must not be null");
        Objects.requireNonNull(valueClass, "Map state value class must not be null");
        return (MapState<K, V>) this.stateCache.computeIfAbsent(cacheKey(MAP_PREFIX, name), key -> {
            MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, keyClass, valueClass);
            enableTtl(descriptor, ttlConfig);
            return this.runtimeContext.getMapState(descriptor);
        });
    }

    public <T> ReducingState<T> reducingState(String name, Class<T> valueClass, ReduceFunction<T> reduceFunction) {
        return this.reducingState(name, valueClass, reduceFunction, this.defaultTtlConfig);
    }

    @SuppressWarnings("unchecked")
    public <T> ReducingState<T> reducingState(
            String name,
            Class<T> valueClass,
            ReduceFunction<T> reduceFunction,
            StateTtlConfig ttlConfig) {
        validateName(name);
        Objects.requireNonNull(valueClass, "Reducing state value class must not be null");
        Objects.requireNonNull(reduceFunction, "Reducing function must not be null");
        return (ReducingState<T>) this.stateCache.computeIfAbsent(cacheKey(REDUCING_PREFIX, name), key -> {
            ReducingStateDescriptor<T> descriptor = new ReducingStateDescriptor<>(name, reduceFunction, valueClass);
            enableTtl(descriptor, ttlConfig);
            return this.runtimeContext.getReducingState(descriptor);
        });
    }

    public <IN, ACC, OUT> AggregatingState<IN, OUT> aggregatingState(
            String name,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            Class<ACC> accumulatorClass) {
        return this.aggregatingState(name, aggregateFunction, accumulatorClass, this.defaultTtlConfig);
    }

    @SuppressWarnings("unchecked")
    public <IN, ACC, OUT> AggregatingState<IN, OUT> aggregatingState(
            String name,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            Class<ACC> accumulatorClass,
            StateTtlConfig ttlConfig) {
        validateName(name);
        Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
        Objects.requireNonNull(accumulatorClass, "Accumulator class must not be null");
        return (AggregatingState<IN, OUT>) this.stateCache.computeIfAbsent(cacheKey(AGGREGATING_PREFIX, name), key -> {
            AggregatingStateDescriptor<IN, ACC, OUT> descriptor =
                    new AggregatingStateDescriptor<>(name, aggregateFunction, accumulatorClass);
            enableTtl(descriptor, ttlConfig);
            return this.runtimeContext.getAggregatingState(descriptor);
        });
    }

    private static String cacheKey(String stateType, String name) {
        return stateType + ":" + name;
    }

    private static void validateName(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("State name must not be blank");
        }
    }

    private static void enableTtl(StateDescriptor<?, ?> descriptor, StateTtlConfig ttlConfig) {
        if (ttlConfig != null) {
            descriptor.enableTimeToLive(ttlConfig);
        }
    }
}
