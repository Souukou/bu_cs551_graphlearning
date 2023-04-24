package graphlearning.maps;

import graphlearning.types.NodeComputationGraph;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

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

    // @Test
    // void flatMap() {
    //     ListCollector<NodeComputationGraph> graphs = new ListCollector<>(out);
    //     flatMapNode.flatMap(nodeIds, graphs);
    // }
}
