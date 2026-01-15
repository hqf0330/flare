package com.bhcode.flare.flink.acc;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Custom Multi-Counter Accumulator for Flink.
 */
public class MultiCounterAccumulator implements SimpleAccumulator<Map<String, Long>> {
    private final Map<String, Long> multiCounter = new ConcurrentHashMap<>();

    @Override
    public void add(Map<String, Long> value) {
        if (value != null) {
            value.forEach(this::add);
        }
    }

    public void add(String key, Long count) {
        if (key != null && count != null) {
            this.multiCounter.merge(key, count, Long::sum);
        }
    }

    @Override
    public Map<String, Long> getLocalValue() {
        return this.multiCounter;
    }

    @Override
    public void resetLocal() {
        this.multiCounter.clear();
    }

    @Override
    public void merge(Accumulator<Map<String, Long>, Map<String, Long>> other) {
        if (other != null) {
            this.add(other.getLocalValue());
        }
    }

    @Override
    public Accumulator<Map<String, Long>, Map<String, Long>> clone() {
        MultiCounterAccumulator clone = new MultiCounterAccumulator();
        clone.add(new HashMap<>(this.multiCounter));
        return clone;
    }
}
