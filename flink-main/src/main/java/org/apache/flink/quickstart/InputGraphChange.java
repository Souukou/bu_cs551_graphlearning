package org.apache.flink.quickstart;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.api.java.tuple.Tuple2;

@Getter
@AllArgsConstructor
public class InputGraphChange {
    private Tuple2<String, Integer> inputGraphChange;
}
