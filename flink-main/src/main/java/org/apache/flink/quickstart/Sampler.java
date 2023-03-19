package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Sampler
        implements MapFunction<List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>> {

    /*
        The reservoir is supposed to store "old" nodes of the graph so that we do not suffer
        from the "catastrophic forgetting" problem when training our GNN.

        Note: instead of a local variable we should instead use ListState (but this only works for keys streams)
        or ListStateDescriptor.
     */
    private List<Tuple2<String, Integer>> reservoir;

    public Sampler(List<Tuple2<String, Integer>> samples) {
        reservoir = samples;
    }
    @Override
    public List<Tuple2<String, Integer>> map(List<Tuple2<String, Integer>> newEvents) {
        List<Tuple2<String, Integer>> trainingSamples = new ArrayList<>();

        // sample some old nodes from the graph
        // reservoir = sample old nodes
        trainingSamples.addAll(newEvents);
        trainingSamples.addAll(reservoir);
        return trainingSamples;
    }
}
