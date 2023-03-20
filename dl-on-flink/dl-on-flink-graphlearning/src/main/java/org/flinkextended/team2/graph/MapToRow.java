package org.flinkextended.team2.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

public class MapToRow implements MapFunction<Tuple5<Integer, Short, Integer, byte[], String>, Row> {
    @Override
    public Tuple5<Integer, Short, Integer, byte[], String> map(Tuple5<Integer, Short, Integer, byte[], String> tuple) {
//        Tuple5<Integer, Short, Integer, byte[], String> tuple = graphChange.getInputGraphChange();
        return Row.of(tuple.f0, tuple.f4, tuple.f2, tuple.f3);
    }

}
