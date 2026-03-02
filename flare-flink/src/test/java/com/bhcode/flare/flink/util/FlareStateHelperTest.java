package com.bhcode.flare.flink.util;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FlareStateHelperTest {

    @Test
    public void testValueStateUsesDefaultTtlAndCaches() {
        RuntimeContextRecorder recorder = new RuntimeContextRecorder();
        FlareStateHelper helper = new FlareStateHelper(
                recorder.runtimeContext(),
                FlareStateHelper.defaultTtlConfig(7)
        );

        ValueState<String> first = helper.valueState("user_state", String.class);
        ValueState<String> second = helper.valueState("user_state", String.class);

        assertSame(first, second);
        assertEquals(1, recorder.valueStateCalls);
        assertNotNull(recorder.lastValueStateDescriptor);

        StateTtlConfig ttlConfig = recorder.lastValueStateDescriptor.getTtlConfig();
        assertTrue(ttlConfig.isEnabled());
        assertEquals(Duration.ofDays(7), ttlConfig.getTimeToLive());
        assertEquals(StateTtlConfig.UpdateType.OnReadAndWrite, ttlConfig.getUpdateType());
        assertEquals(StateTtlConfig.StateVisibility.NeverReturnExpired, ttlConfig.getStateVisibility());
    }

    @Test
    public void testValueStateCanDisableTtl() {
        RuntimeContextRecorder recorder = new RuntimeContextRecorder();
        FlareStateHelper helper = new FlareStateHelper(
                recorder.runtimeContext(),
                FlareStateHelper.defaultTtlConfig(7)
        );

        helper.valueState("no_ttl_state", String.class, null);

        assertNotNull(recorder.lastValueStateDescriptor);
        assertFalse(recorder.lastValueStateDescriptor.getTtlConfig().isEnabled());
    }

    @Test
    public void testStateCachesAreSeparatedByStateType() {
        RuntimeContextRecorder recorder = new RuntimeContextRecorder();
        FlareStateHelper helper = new FlareStateHelper(
                recorder.runtimeContext(),
                FlareStateHelper.defaultTtlConfig(7)
        );

        helper.valueState("same_name", String.class);
        helper.valueState("same_name", String.class);
        helper.listState("same_name", String.class);
        helper.listState("same_name", String.class);
        helper.mapState("same_name", String.class, Long.class);
        helper.mapState("same_name", String.class, Long.class);

        assertEquals(1, recorder.valueStateCalls);
        assertEquals(1, recorder.listStateCalls);
        assertEquals(1, recorder.mapStateCalls);
    }

    @Test
    public void testReducingAndAggregatingStateSupportsCustomTtl() {
        RuntimeContextRecorder recorder = new RuntimeContextRecorder();
        FlareStateHelper helper = new FlareStateHelper(
                recorder.runtimeContext(),
                FlareStateHelper.defaultTtlConfig(7)
        );
        StateTtlConfig customTtl = StateTtlConfig.newBuilder(Duration.ofHours(6)).build();

        helper.reducingState("sum_state", Integer.class, Integer::sum, customTtl);
        helper.aggregatingState("agg_state", new SumAggregate(), Long.class, customTtl);

        assertNotNull(recorder.lastReducingStateDescriptor);
        assertNotNull(recorder.lastAggregatingStateDescriptor);
        assertEquals(Duration.ofHours(6), recorder.lastReducingStateDescriptor.getTtlConfig().getTimeToLive());
        assertEquals(Duration.ofHours(6), recorder.lastAggregatingStateDescriptor.getTtlConfig().getTimeToLive());
    }

    private static class SumAggregate implements AggregateFunction<Long, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Long value, Long accumulator) {
            return value + accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long left, Long right) {
            return left + right;
        }
    }

    private static class RuntimeContextRecorder {

        private final ValueState<?> valueStateProxy = stateProxy(ValueState.class);
        private final ListState<?> listStateProxy = stateProxy(ListState.class);
        private final MapState<?, ?> mapStateProxy = stateProxy(MapState.class);
        private final ReducingState<?> reducingStateProxy = stateProxy(ReducingState.class);
        private final AggregatingState<?, ?> aggregatingStateProxy = stateProxy(AggregatingState.class);

        private int valueStateCalls;
        private int listStateCalls;
        private int mapStateCalls;
        private int reducingStateCalls;
        private int aggregatingStateCalls;

        private ValueStateDescriptor<?> lastValueStateDescriptor;
        private ListStateDescriptor<?> lastListStateDescriptor;
        private MapStateDescriptor<?, ?> lastMapStateDescriptor;
        private ReducingStateDescriptor<?> lastReducingStateDescriptor;
        private AggregatingStateDescriptor<?, ?, ?> lastAggregatingStateDescriptor;

        private RuntimeContext runtimeContext() {
            return (RuntimeContext) Proxy.newProxyInstance(
                    FlareStateHelperTest.class.getClassLoader(),
                    new Class<?>[]{RuntimeContext.class},
                    (proxy, method, args) -> {
                        String methodName = method.getName();
                        if ("getState".equals(methodName)) {
                            this.valueStateCalls++;
                            this.lastValueStateDescriptor = (ValueStateDescriptor<?>) args[0];
                            return this.valueStateProxy;
                        }
                        if ("getListState".equals(methodName)) {
                            this.listStateCalls++;
                            this.lastListStateDescriptor = (ListStateDescriptor<?>) args[0];
                            return this.listStateProxy;
                        }
                        if ("getMapState".equals(methodName)) {
                            this.mapStateCalls++;
                            this.lastMapStateDescriptor = (MapStateDescriptor<?, ?>) args[0];
                            return this.mapStateProxy;
                        }
                        if ("getReducingState".equals(methodName)) {
                            this.reducingStateCalls++;
                            this.lastReducingStateDescriptor = (ReducingStateDescriptor<?>) args[0];
                            return this.reducingStateProxy;
                        }
                        if ("getAggregatingState".equals(methodName)) {
                            this.aggregatingStateCalls++;
                            this.lastAggregatingStateDescriptor = (AggregatingStateDescriptor<?, ?, ?>) args[0];
                            return this.aggregatingStateProxy;
                        }
                        if ("toString".equals(methodName)) {
                            return "RuntimeContextRecorderProxy";
                        }
                        if ("hashCode".equals(methodName)) {
                            return System.identityHashCode(proxy);
                        }
                        if ("equals".equals(methodName)) {
                            return proxy == args[0];
                        }
                        throw new UnsupportedOperationException("Unexpected RuntimeContext method: " + methodName);
                    });
        }

        @SuppressWarnings("unchecked")
        private static <T> T stateProxy(Class<T> stateClass) {
            return (T) Proxy.newProxyInstance(
                    FlareStateHelperTest.class.getClassLoader(),
                    new Class<?>[]{stateClass},
                    (proxy, method, args) -> {
                        String methodName = method.getName();
                        if ("toString".equals(methodName)) {
                            return stateClass.getSimpleName() + "Proxy";
                        }
                        if ("hashCode".equals(methodName)) {
                            return System.identityHashCode(proxy);
                        }
                        if ("equals".equals(methodName)) {
                            return proxy == args[0];
                        }
                        return null;
                    });
        }
    }
}
