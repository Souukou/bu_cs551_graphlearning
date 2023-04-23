package graphlearning.maps;

import org.apache.flink.api.common.functions.MapFunction;
import graphlearning.types.NodeComputationGraph;
import org.apache.flink.types.Row;
import org.w3c.dom.Node;

public class MapComputationGraphToRow implements MapFunction<NodeComputationGraph, Row> {
    @Override
    public Row map(NodeComputationGraph nodeComputationGraph) {
        return nodeComputationGraph.getRow();
    }
}
