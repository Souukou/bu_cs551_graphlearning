package graphlearning.sampling;

import org.apache.flink.api.common.functions.MapFunction;

import com.google.gson.Gson;
import graphlearning.helper.RandomNumbers;
import graphlearning.types.Edge;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

/** Sampler. */
public class Sampler implements MapFunction<List<Edge>, List<Integer>> {

    /**
     * The reservoir stores "old" nodes of the graph so that we do not suffer from the "catastrophic
     * forgetting" problem when training our GNN.
     *
     * <p>Note: instead of a local variable we should instead use Flink state
     */
    private Reservoir reservoir;

    private final Integer numOfSamples;
    private List<Integer> oldNodes;

    public Sampler(Integer numOfSamples, String initialNodesPath) {
        this.numOfSamples = numOfSamples;
        Gson gson = new Gson();
        try {
            Reader reader = new FileReader(initialNodesPath);
            oldNodes = gson.fromJson(reader, Nodes.class).getPt_nodes();
        } catch (IOException e) {
            e.printStackTrace();
            oldNodes = new ArrayList<>();
        }
    }

    @Override
    public List<Integer> map(List<Edge> edges) {
        // find nodes list
        Set<Integer> nodeSet = new HashSet<Integer>();

        edges.stream()
                .map(edge -> Arrays.asList(edge.getSourceNode(), edge.getTargetNode()))
                .flatMap(List::stream)
                .collect(Collectors.toList())
                .forEach(node -> nodeSet.add(node));
        List<Integer> allNodes = new ArrayList<>();
        allNodes.addAll(nodeSet);

        // find new nodes (requires db)
        List<Integer> newNodes =
                allNodes.stream().filter(node -> newNode(node)).collect(Collectors.toList());

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
        if (newNodes.size() < numOfSamples / 2) {
            newSamples = newNodes;
        } else {
            newSamples = sampleNewNodes(newNodes, numOfSamples / 2);
        }

        // sample some old nodes from the graph
        reservoir = new Reservoir(numOfSamples / 2);
        oldNodes.stream().forEach(id -> reservoir.update(id));
        List<Integer> oldSamples = reservoir.getReservoir();

        // update the list of nodes
        newNodes.stream().forEach(id -> oldNodes.add(id));

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
