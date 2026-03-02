package com.bhcode.flare.flink.runtime.control;

import com.bhcode.flare.common.lineage.LineageManager;

import java.util.List;
import java.util.Map;

public interface RuntimeControlService {

    void kill();

    int setConf(Map<String, String> conf);

    CheckpointApplyResult updateCheckpoint(CheckpointControlRequest request);

    List<LineageManager.LineageEdge> lineage();

    Map<String, String> config();

    Map<String, Long> metrics();

    List<LineageManager.LineageEdge> datasource();

    List<RuntimeExceptionSnapshot> exceptions(boolean clear, int limit);

    RuntimeDistributePayload distributeSync();

    int collectLineage(List<LineageManager.LineageEdge> edges);
}
