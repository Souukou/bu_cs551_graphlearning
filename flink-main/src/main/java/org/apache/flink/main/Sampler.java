package org.apache.flink.main;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class Sampler
        implements MapFunction<List<GraphChange>, List<GraphChange>> {

    /*
        The reservoir is supposed to store "old" nodes of the graph so that we do not suffer
        from the "catastrophic forgetting" problem when training our GNN.

        Note: instead of a local variable we should instead use ListState (but this only works for keys streams)
        or ListStateDescriptor.
     */
    private List<GraphChange> reservoir;

    public Sampler(List<GraphChange> samples) {
        reservoir = samples;
    }
    @Override
    public List<GraphChange> map(List<GraphChange> newEvents) {
        List<GraphChange> trainingSamples = new ArrayList<>();

        // sample some old nodes from the graph
        // reservoir = sample old nodes
        trainingSamples.addAll(newEvents);
        trainingSamples.addAll(reservoir);
        return trainingSamples;
    }
}
