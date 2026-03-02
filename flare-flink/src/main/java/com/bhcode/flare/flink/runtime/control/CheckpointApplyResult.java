package com.bhcode.flare.flink.runtime.control;

public record CheckpointApplyResult(
        int updatedKeys,
        boolean runtimeApplied
) {
}
