package com.bhcode.flare.flink.functions;

import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.util.MetricUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Base ProcessFunction with built-in distributed metrics and side output support.
 */
public abstract class FlareRichProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> {

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
}
