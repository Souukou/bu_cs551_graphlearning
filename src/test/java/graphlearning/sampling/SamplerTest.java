package graphlearning.sampling;

import graphlearning.types.Edge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** SamplerTest. */
class SamplerTest {
    private Sampler sampler;

    @BeforeEach
    void setUp() {
        int numOfSamples = 10;
        // If path doesn't exist, we assume the initial nodes to be an empty list
        String initialNodesPath = "";
        sampler = new Sampler(numOfSamples, initialNodesPath);
    }

    @Test
    void map() {
        Edge edge1 = Edge.builder().sourceNode(42).targetNode(17).build();
        Edge edge2 = Edge.builder().sourceNode(42).targetNode(13).build();
        List<Edge> edges = Arrays.asList(edge1, edge2);
        List<Integer> samples = sampler.map(edges);
        assertEquals(samples.get(0), 17);
        assertEquals(samples.get(1), 42);
        assertEquals(samples.get(2), 13);
    }
}
