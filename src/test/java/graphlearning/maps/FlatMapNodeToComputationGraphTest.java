package graphlearning.maps;

import org.apache.flink.api.common.functions.util.ListCollector;
import graphlearning.types.NodeComputationGraph;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.support.hierarchical.Node;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FlatMapNodeToComputationGraphTest {
    private FlatMapNodeToComputationGraph flatMapNode;
    private List<Integer> nodeIds;
    private List<NodeComputationGraph> out;
    @BeforeEach
    void setUp() {
        flatMapNode = new FlatMapNodeToComputationGraph();
        nodeIds = new ArrayList<>();
        nodeIds.add(1);
        nodeIds.add(2);
        out = new ArrayList<>();
    }

    @Test
    void flatMap() {
        ListCollector<NodeComputationGraph> graphs = new ListCollector<>(out);
        flatMapNode.flatMap(nodeIds, graphs);
    }
}