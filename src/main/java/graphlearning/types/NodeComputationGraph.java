package graphlearning.types;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

import java.util.List;
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
