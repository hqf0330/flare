package com.bhcode.flare.flink.util;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A simple sink that collects elements into a static list for testing.
 */
public class TestSink<T> implements SinkFunction<T> {
    private static final List<Object> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(T value, Context context) {
        values.add(value);
    }

    public static <T> List<T> getResults() {
        return (List<T>) new ArrayList<>(values);
    }

    public static void clear() {
        values.clear();
    }
}
