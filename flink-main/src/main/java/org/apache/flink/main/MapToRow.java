package org.apache.flink.main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.types.Row;

public class MapToRow implements MapFunction<GraphChange, Row> {
    @Override
    public Row map(GraphChange graphChange) {
        Tuple5<Integer, Short, Integer, byte[], String> tuple = graphChange.getInputGraphChange();
        return Row.of(tuple.f0, tuple.f4, tuple.f2, tuple.f3);
    }

}
