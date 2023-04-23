package graphlearning.types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** EdgeTest. */
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
                        .sourceEmbedding("test1".getBytes())
                        .targetEmbedding("test2".getBytes())
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
        assertArrayEquals(edge.getSourceEmbedding(), "test1".getBytes());
        assertArrayEquals(edge.getTargetEmbedding(), "test2".getBytes());
        assertEquals(edge.getTimestamp(), "timestamp");
    }
}
