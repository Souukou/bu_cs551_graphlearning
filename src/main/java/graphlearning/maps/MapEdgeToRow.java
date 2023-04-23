package graphlearning.maps;

import org.apache.flink.api.common.functions.MapFunction;
import graphlearning.types.Edge;
import org.apache.flink.types.Row;

import java.util.List;

public class MapEdgeToRow implements MapFunction<Edge, Row> {
    @Override
    public Row map(Edge edge) {
        Integer sourceNode = edge.getSourceNode();
        Integer targetNode = edge.getTargetNode();
        List<Byte> sourceEmbedding = edge.getSourceEmbedding();
        List<Byte> targetEmbedding = edge.getTargetEmbedding();
        Integer sourceLabel = edge.getSourceLabel();
        Integer targetLabel = edge.getTargetLabel();
        String timestamp = edge.getTimestamp();
        return Row.of(
                sourceNode,
                targetNode,
                sourceEmbedding,
                targetEmbedding,
                sourceLabel,
                targetLabel,
                timestamp
        );
    }

}
