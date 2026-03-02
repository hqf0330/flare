package com.bhcode.flare.examples.windowrisk.window.agg;

import com.bhcode.flare.examples.windowrisk.model.acc.UserWindowAccumulator;
import com.bhcode.flare.examples.windowrisk.model.event.OrderEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OrderWindowAggregateFunction
        implements AggregateFunction<OrderEvent, UserWindowAccumulator, UserWindowAccumulator> {

    private final double largeOrderThreshold;

    public OrderWindowAggregateFunction(double largeOrderThreshold) {
        this.largeOrderThreshold = largeOrderThreshold;
    }

    @Override
    public UserWindowAccumulator createAccumulator() {
        return new UserWindowAccumulator();
    }

    @Override
    public UserWindowAccumulator add(OrderEvent value, UserWindowAccumulator accumulator) {
        accumulator.add(value, this.largeOrderThreshold);
        return accumulator;
    }

    @Override
    public UserWindowAccumulator getResult(UserWindowAccumulator accumulator) {
        return accumulator.snapshot();
    }

    @Override
    public UserWindowAccumulator merge(UserWindowAccumulator a, UserWindowAccumulator b) {
        return a.merge(b);
    }
}
