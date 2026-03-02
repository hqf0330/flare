package com.bhcode.flare.flink.runtime.control;

public interface RuntimeCheckpointUpdater {

    boolean apply(CheckpointControlRequest request);
}
