package org.apache.flink.main;

import org.apache.flink.api.common.functions.AggregateFunction;
import java.util.ArrayList;
import java.util.List;

public class MyAggregateFunction
        implements AggregateFunction<GraphChange, List<GraphChange>, List<GraphChange>> {
    @Override
    public List<GraphChange> createAccumulator() {
        return new ArrayList<GraphChange>();
    }

    @Override
    public List<GraphChange> add(GraphChange value, List<GraphChange> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public List<GraphChange> getResult(List<GraphChange> accumulator) {
        return accumulator;
    }

    @Override
    public List<GraphChange> merge(List<GraphChange> acc1, List<GraphChange> acc2) {
        for (GraphChange v : acc2) {
           acc1.add(v);
        }
        return acc1;
    }

}
