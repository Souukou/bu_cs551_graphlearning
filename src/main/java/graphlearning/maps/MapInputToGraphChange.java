package graphlearning.maps;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

import graphlearning.types.Edge;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.List;

/** MapInputToGraphChange. */
public class MapInputToGraphChange implements MapFunction<WikipediaEditEvent, Edge> {
    @Override
    public Edge map(WikipediaEditEvent e) {
        List<Byte> byteListSource =
                Arrays.asList(ArrayUtils.toObject("This is a byte string".getBytes()));
        List<Byte> byteListTarget =
                Arrays.asList(ArrayUtils.toObject("This is another byte string".getBytes()));
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
