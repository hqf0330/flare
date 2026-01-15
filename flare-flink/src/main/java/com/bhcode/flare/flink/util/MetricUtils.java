package com.bhcode.flare.flink.util;

import com.bhcode.flare.flink.acc.MultiCounterAccumulator;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

public class MetricUtils {
    private static final String FLARE_MULTI_ACC = "flare_multi_acc";

    /**
     * Increment a distributed counter (uses both Flink Metrics and Accumulators).
     * 
     * @param ctx RuntimeContext from RichFunction
     * @param name Counter name
     * @param count Increment value
     */
    @SuppressWarnings("unchecked")
    public static void counter(RuntimeContext ctx, String name, long count) {
        if (ctx == null || name == null) return;

        // 1. Flink Metric (Real-time, visible in Dashboard/Prometheus)
        Counter metricCounter = ctx.getMetricGroup().counter(name);
        metricCounter.inc(count);

        // 2. Flink Accumulator (Cumulative, visible after job finishes or in WebUI)
        Accumulator rawAcc = ctx.getAccumulator(FLARE_MULTI_ACC);
        MultiCounterAccumulator acc;
        
        if (rawAcc instanceof MultiCounterAccumulator) {
            acc = (MultiCounterAccumulator) rawAcc;
        } else {
            acc = new MultiCounterAccumulator();
            ctx.addAccumulator(FLARE_MULTI_ACC, acc);
        }
        acc.add(name, count);
    }

    /**
     * Increment by 1.
     */
    public static void counter(RuntimeContext ctx, String name) {
        counter(ctx, name, 1L);
    }
}
