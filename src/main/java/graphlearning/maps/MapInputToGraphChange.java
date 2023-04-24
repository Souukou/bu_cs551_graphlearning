package graphlearning.maps;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

import graphlearning.types.Edge;

/** MapInputToGraphChange. */
public class MapInputToGraphChange implements MapFunction<WikipediaEditEvent, Edge> {
    @Override
    public Edge map(WikipediaEditEvent e) {
        byte[] byteListSource = "This is a byte string".getBytes();
        byte[] byteListTarget = "This is another byte string".getBytes();
        return Edge.builder()
                .sourceNode(42)
                .targetNode(17)
                .sourceLabel(100)
                .targetLabel(101)
                .sourceEmbedding(byteListSource)
                .targetEmbedding(byteListTarget)
                .timestamp("timestamp")
                .build();
    }
}
