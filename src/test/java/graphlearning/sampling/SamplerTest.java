package graphlearning.sampling;

import org.apache.flink.configuration.Configuration;

import graphlearning.types.Edge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** SamplerTest. */
class SamplerTest {
    private Sampler sampler;

    @BeforeEach
    void setUp() {
        int numOfSamples = 10;
        // If path doesn't exist, we assume the initial nodes to be an empty list
        String initialNodesPath = "";
        sampler = new Sampler(numOfSamples, initialNodesPath, "dataset-test");
        try {
            sampler.open(new Configuration());
        } catch (Exception e) {
            System.err.println("Error when open RocksDB: " + e.getMessage());
            e.printStackTrace();
            assertEquals(1, 0);
        }
    }

    // @AfterEach
    // void tearDown() {
    //     // delete the dataset-test directory
    //     try {
    //         FileUtils.deleteDirectory(new File("dataset-test"));
    //     } catch (Exception e) {
    //         System.err.println("Error when delete RocksDB: " + e.getMessage());
    //         e.printStackTrace();
    //         Assertions.assertTrue(false);
    //     }
    // }

    @Test
    void map() {
        Edge edge1 =
                Edge.builder()
                        .sourceNode(42)
                        .targetNode(17)
                        .sourceLabel(1)
                        .targetLabel(2)
                        .sourceEmbedding("test1".getBytes())
                        .targetEmbedding("test2".getBytes())
                        .build();

        Edge edge2 =
                Edge.builder()
                        .sourceNode(42)
                        .targetNode(13)
                        .sourceLabel(1)
                        .targetLabel(3)
                        .sourceEmbedding("test1".getBytes())
                        .targetEmbedding("test3".getBytes())
                        .build();

        List<Edge> edges = Arrays.asList(edge1, edge2);
        List<Integer> samples = sampler.map(edges);
        assertEquals(samples.get(0), 17);
        assertEquals(samples.get(1), 42);
        assertEquals(samples.get(2), 13);
    }
}
