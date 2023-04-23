package graphlearning.types;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** NodeComputationGraphTest. */
class NodeComputationGraphTest {
    private NodeComputationGraph nodeComputationGraph;
    private List<Byte> byteListEmbedding;
    private List<List<Integer>> neighborhood;

    @BeforeEach
    void setUp() {
        nodeComputationGraph = new NodeComputationGraph(2, "2-3|3-4|3-5");
    }

    @Test
    void getTuple() {
        Tuple2<Integer, String> result = nodeComputationGraph.getTuple();
        assertEquals(result.f0, 2);
        assertEquals(result.f1, "2-3|3-4|3-5");
    }

    @Test
    void getId() {
        int id = nodeComputationGraph.getId();
        assertEquals(2, id);
    }

    @Test
    void getComputationGraph() {
        String compGraph = nodeComputationGraph.getComputationGraph();
    }
}
