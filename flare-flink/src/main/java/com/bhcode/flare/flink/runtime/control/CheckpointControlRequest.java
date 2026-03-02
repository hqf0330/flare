package com.bhcode.flare.flink.runtime.control;

public record CheckpointControlRequest(
        Long interval,
        Long timeout,
        Long minPauseBetween
) {
}
