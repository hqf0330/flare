package com.bhcode.flare.flink.functions;

import com.bhcode.flare.flink.util.MetricUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

/**
 * Base FlatMapFunction with built-in distributed metrics support.
 */
public abstract class FlareRichFlatMapFunction<IN, OUT> extends RichFlatMapFunction<IN, OUT> {

    protected void counter(String name, long count) {
        MetricUtils.counter(getRuntimeContext(), name, count);
    }

    protected void counter(String name) {
        this.counter(name, 1L);
    }
}
