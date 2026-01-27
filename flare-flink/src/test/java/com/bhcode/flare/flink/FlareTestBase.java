package com.bhcode.flare.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Before;

/**
 * Base class for Flare Flink unit tests.
 */
public abstract class FlareTestBase extends AbstractTestBase {

    protected StreamExecutionEnvironment env;

    @Before
    public void setupEnv() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(1);
    }

    /**
     * Create a mock source from a list of elements.
     */
    protected <T> org.apache.flink.streaming.api.datastream.DataStreamSource<T> createMockSource(java.util.List<T> data) {
        return env.fromCollection(data);
    }
}
