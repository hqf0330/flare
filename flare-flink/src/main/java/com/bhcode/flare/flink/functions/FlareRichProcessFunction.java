package com.bhcode.flare.flink.functions;

import com.bhcode.flare.flink.FlinkStreaming;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Base ProcessFunction with built-in distributed metrics and side output support.
 */
public abstract class FlareRichProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> {

    protected transient FlareStateHelper stateHelper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
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

    /**
     * Shunt dirty data to the standard side output stream.
     */
    protected void collectDirtyData(Context ctx, String dirtyData) {
        if (ctx != null && dirtyData != null) {
            ctx.output(FlinkStreaming.DIRTY_DATA_TAG, dirtyData);
            this.counter("flare_dirty_total");
        }
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
