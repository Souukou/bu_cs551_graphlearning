package graphlearning.maps;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import graphlearning.types.NodeComputationGraph;

public class MapComputationGraphToRow implements MapFunction<NodeComputationGraph, Row> {
    @Override
    public Row map(NodeComputationGraph nodeComputationGraph) {
        return nodeComputationGraph.getRow();
    }
}
