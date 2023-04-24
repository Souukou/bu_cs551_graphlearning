package graphlearning.types;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** NodeComputationGraph. */
@Getter
@AllArgsConstructor
public class NodeComputationGraph {
    private Integer id;
    private String computationGraph;

    public Tuple2<Integer, String> getTuple() {
        return Tuple2.of(id, computationGraph);
    }

    public Row getRow() {
        return Row.of(id, computationGraph);
    }
}
