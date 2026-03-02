package com.bhcode.flare.examples.windowrisk.window.func;

import com.bhcode.flare.examples.windowrisk.model.acc.UserWindowAccumulator;
import com.bhcode.flare.examples.windowrisk.model.result.UserWindowStat;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserWindowStatProcessWindowFunction
        extends ProcessWindowFunction<UserWindowAccumulator, UserWindowStat, String, TimeWindow> {

    @Override
    public void process(
            String userId,
            Context context,
            Iterable<UserWindowAccumulator> elements,
            Collector<UserWindowStat> out) {
        UserWindowAccumulator acc = elements.iterator().next();
        double average = acc.getOrderCount() == 0 ? 0D : acc.getTotalAmount() / acc.getOrderCount();
        double largeRatio = acc.getOrderCount() == 0 ? 0D : (double) acc.getLargeOrderCount() / acc.getOrderCount();
        out.collect(new UserWindowStat(
                userId,
                context.window().getStart(),
                context.window().getEnd(),
                acc.getOrderCount(),
                acc.getPaidOrderCount(),
                acc.getLargeOrderCount(),
                acc.getTotalAmount(),
                average,
                largeRatio
        ));
    }
}
