package com.bhcode.flare.examples.windowrisk.operator.process;

import com.bhcode.flare.examples.windowrisk.conf.JobConstants;
import com.bhcode.flare.examples.windowrisk.model.result.RiskAlert;
import com.bhcode.flare.examples.windowrisk.model.result.RiskLevel;
import com.bhcode.flare.examples.windowrisk.model.result.UserWindowStat;
import com.bhcode.flare.examples.windowrisk.state.StateKeys;
import com.bhcode.flare.flink.functions.FlareRichProcessFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class UserRiskEvaluateProcessFunction extends FlareRichProcessFunction<UserWindowStat, RiskAlert> {

    private final double highAmountThreshold;
    private final double largeOrderRatioThreshold;
    private final long alertStreakThreshold;

    private transient ValueState<Long> streakState;
    private transient ValueState<Long> lastAlertWindowEndState;

    public UserRiskEvaluateProcessFunction(
            double highAmountThreshold,
            double largeOrderRatioThreshold,
            long alertStreakThreshold) {
        this.highAmountThreshold = highAmountThreshold;
        this.largeOrderRatioThreshold = largeOrderRatioThreshold;
        this.alertStreakThreshold = alertStreakThreshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(JobConstants.RISK_STREAK_TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        this.streakState = valueState(StateKeys.RISK_STREAK, Long.class, ttlConfig);
        this.lastAlertWindowEndState = valueState(StateKeys.LAST_ALERT_WINDOW_END, Long.class, ttlConfig);
    }

    @Override
    public void processElement(UserWindowStat value, Context ctx, Collector<RiskAlert> out) throws Exception {
        boolean riskyAmount = value.totalAmount() >= this.highAmountThreshold;
        boolean riskyRatio = value.largeOrderRatio() >= this.largeOrderRatioThreshold;

        Long historyStreak = this.streakState.value();
        long currentStreak = historyStreak == null ? 0L : historyStreak;
        if (riskyAmount || riskyRatio) {
            currentStreak++;
            this.streakState.update(currentStreak);
        } else {
            currentStreak = 0L;
            this.streakState.clear();
        }

        if (currentStreak < this.alertStreakThreshold) {
            return;
        }

        Long lastAlertWindowEnd = this.lastAlertWindowEndState.value();
        if (lastAlertWindowEnd != null && lastAlertWindowEnd == value.windowEnd()) {
            return;
        }

        RiskLevel level = computeLevel(value, currentStreak);
        String reason = buildReason(riskyAmount, riskyRatio, currentStreak, value);
        out.collect(new RiskAlert(
                value.userId(),
                value.windowEnd(),
                level,
                currentStreak,
                reason,
                value.totalAmount(),
                value.largeOrderRatio()
        ));
        counter("risk_alert_total");
        this.lastAlertWindowEndState.update(value.windowEnd());
    }

    private RiskLevel computeLevel(UserWindowStat value, long streak) {
        if (streak >= 3 || value.totalAmount() >= this.highAmountThreshold * 1.5) {
            return RiskLevel.HIGH;
        }
        if (value.largeOrderRatio() >= this.largeOrderRatioThreshold) {
            return RiskLevel.MEDIUM;
        }
        return RiskLevel.LOW;
    }

    private String buildReason(boolean riskyAmount, boolean riskyRatio, long streak, UserWindowStat stat) {
        StringBuilder reason = new StringBuilder("streak=").append(streak);
        if (riskyAmount) {
            reason.append("|amount=").append(stat.totalAmount());
        }
        if (riskyRatio) {
            reason.append("|largeRatio=").append(stat.largeOrderRatio());
        }
        return reason.toString();
    }
}
