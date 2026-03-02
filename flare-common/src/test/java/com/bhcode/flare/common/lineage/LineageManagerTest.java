package com.bhcode.flare.common.lineage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LineageManagerTest {

    @Before
    public void setUp() {
        LineageManager.clear();
    }

    @Test
    public void shouldAggregateStructuredLineageByEdge() {
        LineageManager.addLineage("Kafka:a", "Flink", "SOURCE");
        LineageManager.addLineage("Kafka:a", "Flink", "SOURCE");
        LineageManager.addLineage("Flink", "JDBC:b", "SINK");

        List<LineageManager.LineageEdge> edges = LineageManager.snapshot();

        Assert.assertEquals(2, edges.size());
        LineageManager.LineageEdge sourceEdge = edges.stream()
                .filter(e -> "Kafka:a".equals(e.source()) && "Flink".equals(e.target()))
                .findFirst()
                .orElseThrow();
        Assert.assertEquals(2L, sourceEdge.count());
        Assert.assertTrue(sourceEdge.lastUpdatedTime() > 0);
    }

    @Test
    public void shouldMergeExternalLineageSnapshot() {
        LineageManager.mergeLineage("Kafka:orders", "Flink", "SOURCE", 4L, 2000L);
        LineageManager.mergeLineage("Kafka:orders", "Flink", "SOURCE", 1L, 1000L);

        List<LineageManager.LineageEdge> edges = LineageManager.snapshot();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(5L, edges.get(0).count());
        Assert.assertEquals(2000L, edges.get(0).lastUpdatedTime());
    }
}
