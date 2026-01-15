package com.bhcode.flare.flink.functions;

import com.bhcode.flare.flink.util.MetricUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Base MapFunction with built-in distributed metrics support.
 */
public abstract class FlareRichMapFunction<IN, OUT> extends RichMapFunction<IN, OUT> {

    /**
     * Increment a distributed counter.
     */
    protected void counter(String name, long count) {
        MetricUtils.counter(getRuntimeContext(), name, count);
    }

    protected void counter(String name) {
        this.counter(name, 1L);
    }
}
