package com.bhcode.flare.examples.windowrisk.model.acc;

import com.bhcode.flare.examples.windowrisk.model.event.OrderEvent;

import java.io.Serializable;

public class UserWindowAccumulator implements Serializable {

    private long orderCount;
    private long paidOrderCount;
    private long largeOrderCount;
    private double totalAmount;

    public void add(OrderEvent event, double largeOrderThreshold) {
        this.orderCount++;
        this.totalAmount += event.amount();
        if (event.isPaid()) {
            this.paidOrderCount++;
        }
        if (event.amount() >= largeOrderThreshold) {
            this.largeOrderCount++;
        }
    }

    public UserWindowAccumulator merge(UserWindowAccumulator other) {
        this.orderCount += other.orderCount;
        this.paidOrderCount += other.paidOrderCount;
        this.largeOrderCount += other.largeOrderCount;
        this.totalAmount += other.totalAmount;
        return this;
    }

    public UserWindowAccumulator snapshot() {
        UserWindowAccumulator copy = new UserWindowAccumulator();
        copy.orderCount = this.orderCount;
        copy.paidOrderCount = this.paidOrderCount;
        copy.largeOrderCount = this.largeOrderCount;
        copy.totalAmount = this.totalAmount;
        return copy;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public long getPaidOrderCount() {
        return paidOrderCount;
    }

    public long getLargeOrderCount() {
        return largeOrderCount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }
}
