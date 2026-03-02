package com.bhcode.flare.flink.runtime.control;

public record RuntimeDistributePayload(
        String module,
        String payload,
        long version,
        long timestamp
) {
}
