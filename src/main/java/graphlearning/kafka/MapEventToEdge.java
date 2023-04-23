package graphlearning.kafka;

import org.apache.flink.api.common.functions.MapFunction;

import graphlearning.kafka.protos.Event;
import graphlearning.types.Edge;

/** MapEventToEdge. */
public class MapEventToEdge implements MapFunction<Event, Edge> {

    @Override
    public Edge map(Event event) throws Exception {
        byte[] sourceDataBytes = event.getSourceData().toByteArray();
        byte[] targetDataBytes = event.getTargetData().toByteArray();

        byte[] sourceDataList = sourceDataBytes;
        byte[] targetDataList = targetDataBytes;
        Edge edge =
                Edge.builder()
                        .sourceNode(event.getSource())
                        .targetNode(event.getTarget())
                        .sourceLabel(event.getSourceLabel())
                        .targetLabel(event.getTargetLabel())
                        .sourceEmbedding(sourceDataList)
                        .targetEmbedding(targetDataList)
                        .timestamp(event.getTimestamp().toString())
                        .build();
        return edge;
    }
}
