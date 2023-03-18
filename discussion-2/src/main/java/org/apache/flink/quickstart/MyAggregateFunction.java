package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class MyAggregateFunction
        implements AggregateFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>> {
    @Override
    public List<Tuple2<String, Integer>> createAccumulator() {
        return new ArrayList<Tuple2<String, Integer>>();
    }

    @Override
    public List<Tuple2<String, Integer>> add(Tuple2<String, Integer> value, List<Tuple2<String, Integer>> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public List<Tuple2<String, Integer>> getResult(List<Tuple2<String, Integer>>accumulator) {
        return accumulator;
    }

    @Override
    public List<Tuple2<String, Integer>> merge(List<Tuple2<String, Integer>> acc1, List<Tuple2<String, Integer>> acc2) {
        for (Tuple2<String, Integer> v : acc2) {
           acc1.add(v);
        }
        return acc1;
    }

}
