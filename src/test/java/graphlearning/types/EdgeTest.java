package graphlearning.types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class EdgeTest {
    private Edge edge;

    @BeforeEach
    void setUp() {
        edge =
                Edge.builder()
                        .sourceNode(42)
                        .targetNode(17)
                        .sourceLabel(100)
                        .targetLabel(101)
                        .sourceEmbedding(new ArrayList<>())
                        .targetEmbedding(new ArrayList<>())
                        .timestamp("timestamp")
                        .build();
    }

    @Test
    void testToString() {
        assertEquals(edge.toString(), "(42, 17, timestamp)");
    }

    @Test
    void testGetters() {
        assertEquals(edge.getSourceNode(), 42);
        assertEquals(edge.getTargetNode(), 17);
        assertEquals(edge.getSourceLabel(), 100);
        assertEquals(edge.getTargetLabel(), 101);
        assertTrue(edge.getSourceEmbedding().isEmpty());
        assertTrue(edge.getTargetEmbedding().isEmpty());
        assertEquals(edge.getTimestamp(), "timestamp");
    }
}
