package com.bhcode.flare.examples.windowrisk.model.event;

public record OrderEvent(
        String orderId,
        String userId,
        String merchantId,
        long eventTime,
        double amount,
        String status) {

    public boolean isPaid() {
        return "PAID".equalsIgnoreCase(this.status);
    }
}
