package com.bhcode.flare.examples.windowrisk.conf;

import java.time.Duration;

public final class JobConstants {

    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    public static final Duration IDLE_TIMEOUT = Duration.ofSeconds(30);

    public static final Duration WINDOW_SIZE = Duration.ofMinutes(10);
    public static final Duration WINDOW_SLIDE = Duration.ofMinutes(1);
    public static final long EARLY_FIRE_COUNT = 4L;

    public static final double LARGE_ORDER_THRESHOLD = 500D;
    public static final double WINDOW_HIGH_AMOUNT_THRESHOLD = 2_000D;
    public static final double WINDOW_LARGE_ORDER_RATIO_THRESHOLD = 0.35D;
    public static final long ALERT_STREAK_THRESHOLD = 2L;

    public static final Duration ORDER_DEDUP_TTL = Duration.ofHours(12);
    public static final Duration RISK_STREAK_TTL = Duration.ofDays(3);

    private JobConstants() {
    }
}
