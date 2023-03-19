package org.apache.flink.quickstart;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.api.java.tuple.Tuple2;

@Getter
@AllArgsConstructor
public class GraphChange {
    private Tuple2<String, Integer> inputGraphChange;

    public String toString() {
        return "("
                + inputGraphChange.f0
                + ", "
                + inputGraphChange.f1
                + ")";
    }
}
