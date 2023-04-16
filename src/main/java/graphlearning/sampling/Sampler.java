package graphlearning.sampling;

import org.apache.flink.api.common.functions.MapFunction;

import graphlearning.helper.RandomNumbers;
import graphlearning.types.Edge;

import java.util.*;
import java.util.stream.Collectors;

public class Sampler implements MapFunction<List<Edge>, List<Integer>> {

    /**
     * The reservoir stores "old" nodes of the graph so that we do not suffer from the "catastrophic
     * forgetting" problem when training our GNN.
     *
     * <p>Note: instead of a local variable we should instead use Flink state
     */
    private Reservoir reservoir = new Reservoir();

    @Override
    public List<Integer> map(List<Edge> edges) {
        // find nodes list
        Set<Integer> nodeSet = new HashSet<Integer>();

        edges.stream()
                .map(edge -> Arrays.asList(edge.getSourceNode(), edge.getTargetNode()))
                .flatMap(List::stream)
                .collect(Collectors.toList())
                .forEach(node -> nodeSet.add(node));
        List<Integer> nodes = new ArrayList<>();
        nodes.addAll(nodeSet);

        // filter old nodes (requires db)
        List<Integer> newNodes =
                nodes.stream().filter(node -> newNode(node)).collect(Collectors.toList());

        // insert edges into database
        edges.stream().forEach(edge -> insertEdge(edge.getSourceNode(), edge.getTargetNode()));
        // insert new nodes into database (requires db)
        for (Integer node : newNodes) {
            for (Edge edge : edges) {
                if (edge.getSourceNode().equals(node)) {
                    insertNode(node, edge.getSourceLabel(), edge.getSourceEmbedding());
                    break;
                }
                if (edge.getTargetNode().equals(node)) {
                    insertNode(node, edge.getTargetLabel(), edge.getTargetEmbedding());
                    break;
                }
            }
        }

        // sample new nodes
        List<Integer> newSamples = new ArrayList<>();
        if (newNodes.size() < reservoir.getSize() / 2) {
            newSamples = newNodes;
        } else {
            newSamples = sampleNewNodes(newNodes, reservoir.getSize() / 2);
        }

        // sample some old nodes from the reservoir
        List<Integer> oldSamples = reservoir.sample(reservoir.getSize() / 2);

        // update the reservoir
        newNodes.stream().forEach(node -> reservoir.update(node));

        // concatenate new and old nodes into a single list
        List<Integer> sampledNodes = new ArrayList<>();
        sampledNodes.addAll(newSamples);
        sampledNodes.addAll(oldSamples);

        return sampledNodes;
    }

    private boolean newNode(Integer node) {
        /*
           If node is already in the graph, return true.
           Otherwise, return false.
           (Here we need a call to the database api)
        */
        return true;
    }

    private void insertNode(Integer id, Integer label, List<Byte> embedding) {
        return;
    }

    private void insertEdge(Integer sourceId, Integer targetId) {
        return;
    }

    private List<Integer> sampleNewNodes(List<Integer> newNodes, int numOfSamples) {
        List<Integer> indices = RandomNumbers.randomNumbers(0, newNodes.size() - 1, numOfSamples);
        List<Integer> samples =
                indices.stream().map(i -> newNodes.get(i)).collect(Collectors.toList());
        return samples;
    }
}
