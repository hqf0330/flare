package com.bhcode.flare.examples.windowrisk.window.trigger;

import com.bhcode.flare.examples.windowrisk.state.StateKeys;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class CountOrEndOfWindowTrigger<T> extends Trigger<T, TimeWindow> {

    private final long earlyFireCount;
    private final ValueStateDescriptor<Long> countStateDesc =
            new ValueStateDescriptor<>(StateKeys.TRIGGER_EARLY_FIRE_COUNT, Long.class);

    private CountOrEndOfWindowTrigger(long earlyFireCount) {
        this.earlyFireCount = earlyFireCount;
    }

    public static <T> CountOrEndOfWindowTrigger<T> of(long earlyFireCount) {
        if (earlyFireCount <= 0) {
            throw new IllegalArgumentException("earlyFireCount must be greater than 0");
        }
        return new CountOrEndOfWindowTrigger<>(earlyFireCount);
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Long> countState = ctx.getPartitionedState(countStateDesc);
        long current = countState.value() == null ? 0L : countState.value();
        long updated = current + 1;
        if (updated >= this.earlyFireCount) {
            countState.update(0L);
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.FIRE;
        }
        countState.update(updated);
        ctx.registerEventTimeTimer(window.maxTimestamp());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        ctx.getPartitionedState(countStateDesc).clear();
    }
}
