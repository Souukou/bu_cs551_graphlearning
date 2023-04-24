package graphlearning.maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import graphlearning.rocksdb.RocksDBReader;
import graphlearning.types.NodeComputationGraph;

import java.util.ArrayList;
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
        RocksDBReader reader = new RocksDBReader();
        ArrayList<ArrayList<Integer>> neighbors = reader.getKNeighborIdReservoir(nodeId, 3, -1);
        if (neighbors.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < neighbors.size(); i++) {
            for (int j = 0; j < neighbors.get(i).size(); j++) {
                sb.append(String.format("%d-%d|", i + 1, neighbors.get(i).get(j)));
            }
        }
        sb = new StringBuilder(sb.substring(0, sb.length() - 1));
        reader.finalize();
        return sb.toString();
    }
}
