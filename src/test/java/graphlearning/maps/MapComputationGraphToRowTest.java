package graphlearning.maps;

import org.apache.flink.types.Row;

import graphlearning.types.NodeComputationGraph;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapComputationGraphToRowTest {
    private MapComputationGraphToRow mapComputationGraphToRow;
    private NodeComputationGraph nodeComputationGraph;

    @BeforeEach
    void setUp() {
        mapComputationGraphToRow = new MapComputationGraphToRow();
        nodeComputationGraph = new NodeComputationGraph(1, "1-2|1-3");
    }

    @Test
    void map() {
        Row row = mapComputationGraphToRow.map(nodeComputationGraph);
        assertEquals(row.getField(0), 1);
        assertEquals(row.getField(1), "1-2|1-3");
    }
}
