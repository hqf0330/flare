package com.bhcode.flare.examples;

import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.examples.windowrisk.conf.JobConstants;
import com.bhcode.flare.examples.windowrisk.model.event.OrderEvent;
import com.bhcode.flare.examples.windowrisk.model.result.RiskAlert;
import com.bhcode.flare.examples.windowrisk.model.result.UserWindowStat;
import com.bhcode.flare.examples.windowrisk.operator.process.OrderValidateProcessFunction;
import com.bhcode.flare.examples.windowrisk.operator.process.UserRiskEvaluateProcessFunction;
import com.bhcode.flare.examples.windowrisk.window.agg.OrderWindowAggregateFunction;
import com.bhcode.flare.examples.windowrisk.window.func.UserWindowStatProcessWindowFunction;
import com.bhcode.flare.examples.windowrisk.window.trigger.CountOrEndOfWindowTrigger;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import java.util.Arrays;
import java.util.List;

/**
 * 企业级示例（复杂版）：
 * 1) 乱序事件时间处理（水位线）
 * 2) 订单校验 + 去重 + 脏数据分流
 * 3) 自定义 Trigger 的滑动窗口统计
 * 4) 基于状态的连续风险告警
 */
@Slf4j
@Streaming(interval = 30, parallelism = 2)
public class WindowWatermarkStateTask extends FlinkStreaming {

    @Override
    public void process() {
        DataStream<OrderEvent> source = this.getEnv().fromCollection(mockEvents());

        // 1) 分配 watermark：允许乱序 + 空闲分区探测
        DataStream<OrderEvent> withWatermark = source.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(JobConstants.MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner((event, ts) -> event.eventTime())
                        .withIdleness(JobConstants.IDLE_TIMEOUT)
        );

        // 2) 数据清洗与去重（按 orderId 键控以启用 KeyedState）
        SingleOutputStreamOperator<OrderEvent> validOrders = withWatermark
                .keyBy(OrderEvent::orderId)
                .process(new OrderValidateProcessFunction());

        // 3) 事件时间滑动窗口 + 自定义 trigger（按 userId 聚合）
        DataStream<UserWindowStat> userWindowStats = validOrders
                .keyBy(OrderEvent::userId)
                .window(SlidingEventTimeWindows.of(JobConstants.WINDOW_SIZE, JobConstants.WINDOW_SLIDE))
                .trigger(CountOrEndOfWindowTrigger.of(JobConstants.EARLY_FIRE_COUNT))
                .aggregate(
                        new OrderWindowAggregateFunction(JobConstants.LARGE_ORDER_THRESHOLD),
                        new UserWindowStatProcessWindowFunction()
                );

        // 4) 连续风险告警
        DataStream<RiskAlert> alerts = userWindowStats
                .keyBy(UserWindowStat::userId)
                .process(new UserRiskEvaluateProcessFunction(
                        JobConstants.WINDOW_HIGH_AMOUNT_THRESHOLD,
                        JobConstants.WINDOW_LARGE_ORDER_RATIO_THRESHOLD,
                        JobConstants.ALERT_STREAK_THRESHOLD
                ));

        this.uname(validOrders, "order_valid_stream", "Valid Orders").print("valid-orders");
        this.uname(userWindowStats, "user_window_stats", "User Window Stats").print("window-stats");
        this.uname(alerts, "risk_alerts", "Risk Alerts").print("risk-alerts");
        this.printDirtyData(validOrders);
    }

    private List<OrderEvent> mockEvents() {
        long now = System.currentTimeMillis();
        return Arrays.asList(
                new OrderEvent("o-1001", "u1", "m1", now - 9 * 60_000L, 520D, "PAID"),
                new OrderEvent("o-1002", "u1", "m1", now - 8 * 60_000L, 860D, "PAID"),
                new OrderEvent("o-1003", "u2", "m2", now - 7 * 60_000L, 120D, "PAID"),
                new OrderEvent("o-1004", "u1", "m3", now - 6 * 60_000L, 980D, "PAID"),
                new OrderEvent("o-1004", "u1", "m3", now - 6 * 60_000L, 980D, "PAID"), // 重复订单
                new OrderEvent("o-1005", "u3", "m2", now - 5 * 60_000L + 2_000L, 50D, "REFUND"), // 轻微乱序
                new OrderEvent("o-1006", "u2", "m4", now - 4 * 60_000L, 1_250D, "PAID"),
                new OrderEvent("o-1007", "u1", "m5", now - 3 * 60_000L, 930D, "PAID"),
                new OrderEvent("o-1008", "u1", "m5", now - 2 * 60_000L, -30D, "PAID"), // 非法金额
                new OrderEvent("o-1009", "u1", "m6", now - 60_000L, 1_100D, "PAID"),
                new OrderEvent("o-1010", "", "m2", now - 30_000L, 280D, "PAID"), // 非法 user
                new OrderEvent("o-1011", "u2", "m2", now - 20_000L, 620D, "UNKNOWN") // 非法状态
        );
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(WindowWatermarkStateTask.class, args);
    }
}
