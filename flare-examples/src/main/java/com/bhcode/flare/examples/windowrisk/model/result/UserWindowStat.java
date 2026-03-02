package com.bhcode.flare.examples.windowrisk.model.result;

public record UserWindowStat(
        String userId,
        long windowStart,
        long windowEnd,
        long orderCount,
        long paidOrderCount,
        long largeOrderCount,
        double totalAmount,
        double averageAmount,
        double largeOrderRatio) {
}
