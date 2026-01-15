package com.bhcode.flare.common.lineage;

import lombok.extern.slf4j.Slf4j;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class LineageManager {
    private static final Set<String> lineages = ConcurrentHashMap.newKeySet();

    public static void addLineage(String source, String target, String operation) {
        String entry = String.format("[Lineage] %s -> %s (%s)", source, target, operation);
        if (lineages.add(entry)) {
            log.info("Collected new lineage: {}", entry);
        }
    }

    public static void show() {
        if (lineages.isEmpty()) {
            log.info("No lineage information collected.");
            return;
        }
        log.info("=== Flare Lineage Information ===");
        lineages.forEach(log::info);
        log.info("=================================");
    }
}
