package graphlearning.window;

import graphlearning.types.Edge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateToListTest {
    private AggregateToList aggregateToList;
    private Edge edge;

    @BeforeEach
    void setUp() {
        aggregateToList = new AggregateToList();
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
    void testCreateAccumulator() {
        assertTrue(aggregateToList.createAccumulator().isEmpty());
    }

    @Test
    void testAdd() {
        List<Edge> acc = new ArrayList<>();

        List<Edge> result = aggregateToList.add(edge, acc);

        assertEquals(result, Arrays.asList(edge));
    }

    @Test
    void testGetResult() {
        Edge edge =
                Edge.builder()
                        .sourceNode(42)
                        .targetNode(17)
                        .sourceLabel(100)
                        .targetLabel(101)
                        .sourceEmbedding("test1".getBytes())
                        .targetEmbedding("test2".getBytes())
                        .timestamp("timestamp")
                        .build();

        List<Edge> acc = Arrays.asList(edge);

        List<Edge> result = aggregateToList.getResult(acc);

        assertEquals(result, Arrays.asList(edge));
    }

    @Test
    void testMerge() {
        Edge val1 = edge;
        Edge val2 = edge;
        Edge val3 = edge;

        List<Edge> acc1 = new ArrayList<>();
        acc1.add(val1);
        acc1.add(val2);
        List<Edge> acc2 = Arrays.asList(val3);

        List<Edge> result = aggregateToList.merge(acc1, acc2);

        assertEquals(result, Arrays.asList(val1, val2, val3));
    }
}
