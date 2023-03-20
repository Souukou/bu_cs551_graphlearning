package org.apache.flink.main;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

@Getter
@AllArgsConstructor
public class GraphChange {
    /*
        SourceNode
     */
    private Tuple5<Integer, Short, Integer, byte[], String> inputGraphChange;

    public String toString() {
        return "("
                + inputGraphChange.f0
                + ", "
                + inputGraphChange.f1
                + ")";
    }
}
