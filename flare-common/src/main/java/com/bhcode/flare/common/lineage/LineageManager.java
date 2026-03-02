package com.bhcode.flare.common.lineage;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class LineageManager {

    private static final Map<LineageKey, LineageStat> LINEAGES = new ConcurrentHashMap<>();

    private LineageManager() {
    }

    public static void addLineage(String source, String target, String operation) {
        mergeLineage(source, target, operation, 1L, System.currentTimeMillis());
    }

    /**
     * 合并外部采集到的血缘边（支持批量计数合并）。
     */
    public static void mergeLineage(
            String source,
            String target,
            String operation,
            long count,
            long lastUpdatedTime) {
        String finalSource = normalize(source);
        String finalTarget = normalize(target);
        String finalOperation = normalize(operation);
        LineageKey key = new LineageKey(finalSource, finalTarget, finalOperation);
        long safeCount = count <= 0 ? 1L : count;
        long updateTime = lastUpdatedTime > 0 ? lastUpdatedTime : System.currentTimeMillis();

        LineageStat stat = LINEAGES.computeIfAbsent(key, ignored -> {
            log.info("Collected new lineage: [Lineage] {} -> {} ({})", finalSource, finalTarget, finalOperation);
            return new LineageStat();
        });
        stat.merge(safeCount, updateTime);
    }

    /**
     * 返回结构化血缘快照。
     */
    public static List<LineageEdge> snapshot() {
        List<LineageEdge> snapshot = new ArrayList<>(LINEAGES.size());
        for (Map.Entry<LineageKey, LineageStat> entry : LINEAGES.entrySet()) {
            LineageKey key = entry.getKey();
            LineageStat stat = entry.getValue();
            snapshot.add(new LineageEdge(
                    key.source(),
                    key.target(),
                    key.operation(),
                    stat.count.get(),
                    stat.lastUpdatedTime.get()
            ));
        }
        snapshot.sort(Comparator
                .comparing(LineageEdge::source)
                .thenComparing(LineageEdge::target)
                .thenComparing(LineageEdge::operation));
        return snapshot;
    }

    /**
     * 清空血缘缓存（测试用途）。
     */
    public static void clear() {
        LINEAGES.clear();
    }

    public static void show() {
        List<LineageEdge> snapshot = snapshot();
        if (snapshot.isEmpty()) {
            log.info("No lineage information collected.");
            return;
        }
        log.info("=== Flare Lineage Information ===");
        for (LineageEdge edge : snapshot) {
            log.info("[Lineage] {} -> {} ({}) count={} lastUpdated={}",
                    edge.source(),
                    edge.target(),
                    edge.operation(),
                    edge.count(),
                    edge.lastUpdatedTime());
        }
        log.info("=================================");
    }

    private static String normalize(String value) {
        if (value == null) {
            return "unknown";
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? "unknown" : trimmed;
    }

    public record LineageEdge(
            String source,
            String target,
            String operation,
            long count,
            long lastUpdatedTime
    ) {
    }

    private record LineageKey(String source, String target, String operation) {
    }

    private static final class LineageStat {
        private final AtomicLong count = new AtomicLong(0);
        private final AtomicLong lastUpdatedTime = new AtomicLong(0);

        private void merge(long c, long time) {
            this.count.addAndGet(c);
            this.lastUpdatedTime.updateAndGet(old -> Math.max(old, time));
        }
    }
}
