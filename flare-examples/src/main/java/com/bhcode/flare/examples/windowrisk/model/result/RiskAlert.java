package com.bhcode.flare.examples.windowrisk.model.result;

public record RiskAlert(
        String userId,
        long windowEnd,
        RiskLevel riskLevel,
        long riskStreak,
        String reason,
        double totalAmount,
        double largeOrderRatio) {
}
