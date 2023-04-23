package graphlearning.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import graphlearning.types.NodeComputationGraph;

import java.util.List;

/** FlatMapNodeToComputationGraph. */
public class FlatMapNodeToComputationGraph
        implements FlatMapFunction<List<Integer>, NodeComputationGraph> {
    @Override
    public void flatMap(List<Integer> ids, Collector<NodeComputationGraph> out) {
        ids.stream()
                .forEach(
                        nodeId -> {
                            String computationGraph = kNeighbors(nodeId);
                            NodeComputationGraph nodeComputationGraph =
                                    new NodeComputationGraph(nodeId, computationGraph);
                            out.collect(nodeComputationGraph);
                        });
    }

    private String kNeighbors(Integer nodeId) {
        return "1-2|1-3|2-4|2-5|3-6|3-7";
    }
}
