package com.bhcode.flare.examples.windowrisk.operator.process;

import com.bhcode.flare.examples.windowrisk.conf.JobConstants;
import com.bhcode.flare.examples.windowrisk.model.event.OrderEvent;
import com.bhcode.flare.examples.windowrisk.state.StateKeys;
import com.bhcode.flare.flink.functions.FlareRichProcessFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class OrderValidateProcessFunction extends FlareRichProcessFunction<OrderEvent, OrderEvent> {

    private transient MapState<String, Long> dedupOrderState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(JobConstants.ORDER_DEDUP_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        this.dedupOrderState = mapState(StateKeys.ORDER_DEDUP_MAP, String.class, Long.class, ttlConfig);
    }

    @Override
    public void processElement(OrderEvent value, Context ctx, Collector<OrderEvent> out) throws Exception {
        if (!isValid(value)) {
            collectDirtyData(ctx, "invalid_order|" + value);
            return;
        }

        if (this.dedupOrderState.contains(value.orderId())) {
            collectDirtyData(ctx, "duplicate_order|" + value.orderId());
            return;
        }

        this.dedupOrderState.put(value.orderId(), value.eventTime());
        counter("order_valid_total");
        out.collect(value);
    }

    private boolean isValid(OrderEvent value) {
        if (value == null) {
            return false;
        }
        if (isBlank(value.orderId()) || isBlank(value.userId()) || isBlank(value.merchantId())) {
            return false;
        }
        if (value.eventTime() <= 0 || value.amount() <= 0) {
            return false;
        }
        return "PAID".equalsIgnoreCase(value.status()) || "REFUND".equalsIgnoreCase(value.status());
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
