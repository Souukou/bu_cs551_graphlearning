package graphlearning.window;

import org.apache.flink.api.common.functions.AggregateFunction;

import graphlearning.types.Edge;

import java.util.ArrayList;
import java.util.List;

/** AggregateToList. */
public class AggregateToList implements AggregateFunction<Edge, List<Edge>, List<Edge>> {
    @Override
    public List<Edge> createAccumulator() {
        return new ArrayList<Edge>();
    }

    @Override
    public List<Edge> add(Edge value, List<Edge> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public List<Edge> getResult(List<Edge> accumulator) {
        return accumulator;
    }

    @Override
    public List<Edge> merge(List<Edge> acc1, List<Edge> acc2) {
        for (Edge v : acc2) {
            acc1.add(v);
        }
        return acc1;
    }
}
