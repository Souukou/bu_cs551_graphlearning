package graphlearning.maps;

import org.apache.flink.types.Row;

import graphlearning.types.Edge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MapEdgeToRowTest {
    private Edge edge;
    private MapEdgeToRow mapEdgeToRow;

    @BeforeEach
    void setUp() {
        mapEdgeToRow = new MapEdgeToRow();
        edge =
                Edge.builder()
                        .sourceNode(42)
                        .targetNode(17)
                        .sourceLabel(100)
                        .targetLabel(101)
                        .sourceEmbedding("test".getBytes())
                        .targetEmbedding("test2".getBytes())
                        .timestamp("timestamp")
                        .build();
    }

    @Test
    void testMap() {
        Row row = mapEdgeToRow.map(edge);
        assertEquals(row.getField(0), edge.getSourceNode());
        assertEquals(row.getField(1), edge.getTargetNode());
        assertEquals(row.getField(4), edge.getSourceLabel());
    }
}
