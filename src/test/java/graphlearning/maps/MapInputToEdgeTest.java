package graphlearning.maps;

import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

import graphlearning.types.Edge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.mock;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MapInputToEdgeTest {
    private WikipediaEditEvent editEvent;
    private MapInputToGraphChange myMap;
    private Edge edge;

    @BeforeEach
    void setUp() {
        editEvent = mock(WikipediaEditEvent.class);
        myMap = new MapInputToGraphChange();
        byte[] byteListSource = "This is a byte string".getBytes();
        byte[] byteListTarget = "This is another byte string".getBytes();

        edge =
                Edge.builder()
                        .sourceNode(42)
                        .targetNode(17)
                        .sourceLabel(100)
                        .targetLabel(101)
                        .sourceEmbedding(byteListSource)
                        .targetEmbedding(byteListTarget)
                        .timestamp("timestamp")
                        .build();
    }

    @Test
    void testMap() {
        Edge result = myMap.map(editEvent);
        assertEquals(result.toString(), edge.toString());
    }
}
