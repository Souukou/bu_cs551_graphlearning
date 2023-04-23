package graphlearning.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * RocksDB read only interface to pull up node neighbors and features from RocksDB. By default, it
 * will read from the dataset-test/node.db and dataset-test/edge.db
 */
public class RocksDBReader {
    private String nodeDbPath;
    private String edgeDbPath;

    private RocksDB nodeDb;
    private RocksDB edgeDb;

    /**
     * RocksDBReader constructor with costomized node and edge db path.
     *
     * @param nodeDbPath
     * @param edgeDbPath
     */
    public RocksDBReader(String nodeDbPath, String edgeDbPath) {
        this.nodeDbPath = nodeDbPath;
        this.edgeDbPath = edgeDbPath;
        this.open();
    }

    /**
     * RocksDBReader constructor with default node and edge db path.
     *
     * <p>default node db path: dataset-test/node.db
     *
     * <p>default edge db path: dataset-test/edge.db
     */
    public RocksDBReader() {
        this.nodeDbPath = "dataset-test/node.db";
        this.edgeDbPath = "dataset-test/edge.db";
        this.open();
    }

    private void open() {
        try {
            nodeDb = RocksDB.openReadOnly(nodeDbPath);
            edgeDb = RocksDB.openReadOnly(edgeDbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /**
     * Query the node's K neighbors ID from edge.db.
     *
     * @param nodeId query node's id
     * @param k {@code int} the number of levels of neighbors to be queried.
     * @param maxNumOfNeighbors {@code int} the maximum number of neighbors for each node.
     * @return {@code ArrayList&lt;ArrayList&lt;ArrayList&lt;Integer&gt;&gt;&gt;} contains a list of
     *     neighbors' id.
     */
    public ArrayList<ArrayList<Integer>> getKNeighborIdPlain(
            Integer nodeId, int k, int maxNumOfNeighbors) {
        HashSet<ArrayList<Integer>> kNeighbors = new HashSet<ArrayList<Integer>>();
        ArrayList<Integer> thisLevelNodeIds = new ArrayList<Integer>(Arrays.asList(nodeId));
        while (k != 0) {
            HashSet<Integer> thisLevelNeighbors = new HashSet<Integer>();
            for (Integer node : thisLevelNodeIds) {

                ArrayList<Integer> neighbors =
                        NeighborReader.findNeighbor(node, edgeDb, maxNumOfNeighbors);

                for (Integer neighbor : neighbors) {
                    kNeighbors.add(new ArrayList<Integer>(Arrays.asList(node, neighbor)));
                }
                thisLevelNeighbors.addAll(neighbors);
            }
            thisLevelNodeIds = new ArrayList<Integer>(thisLevelNeighbors);
            k--;
        }
        return new ArrayList<ArrayList<Integer>>(kNeighbors);
    }

    /**
     * Query the node's K neighbors ID from edge.db.
     *
     * @param nodeId query node's id
     * @param k {@code int} the number of levels of neighbors to be queried.
     * @param maxNumOfNeighbors {@code int} the maximum number of neighbors for each node.
     * @return {@code ArrayList&lt;ArrayList&lt;ArrayList&lt;Integer&gt;&gt;&gt;} contains a list of
     *     neighbors' id.
     */
    public ArrayList<ArrayList<Integer>> getKNeighborIdReservoir(
            Integer nodeId, int k, int maxNumOfNeighbors) {
        HashSet<ArrayList<Integer>> kNeighbors = new HashSet<ArrayList<Integer>>();
        ArrayList<Integer> thisLevelNodeIds = new ArrayList<Integer>(Arrays.asList(nodeId));
        while (k != 0) {
            HashSet<Integer> thisLevelNeighbors = new HashSet<Integer>();
            for (Integer node : thisLevelNodeIds) {

                ArrayList<Integer> neighbors =
                        NeighborReader.findNeighborReservoir(node, edgeDb, maxNumOfNeighbors);

                for (Integer neighbor : neighbors) {
                    kNeighbors.add(new ArrayList<Integer>(Arrays.asList(node, neighbor)));
                }
                thisLevelNeighbors.addAll(neighbors);
            }
            thisLevelNodeIds = new ArrayList<Integer>(thisLevelNeighbors);
            k--;
        }
        return new ArrayList<ArrayList<Integer>>(kNeighbors);
    }

    /**
     * Get random sampling from whole DB.
     *
     * @param n number of
     * @return {@code ArrayList&lt;Integer&gt;} contains a list of node id.
     */
    public ArrayList<Integer> getRandomSample(int n) {
        ArrayList<Integer> sample = new ArrayList<Integer>();
        try (RocksIterator iterator = nodeDb.newIterator()) {
            // Iterate through all the keys in the database
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                // Convert the key to a int
                int key = Integer.parseInt(new String(iterator.key()));
                sample.add(key);
            }
        }
        Collections.shuffle(sample);
        List<Integer> sublist = sample.subList(0, n);
        return new ArrayList<Integer>(sublist);
    }

    public void finalize() {
        nodeDb.close();
        edgeDb.close();
    }
}
