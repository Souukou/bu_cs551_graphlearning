package graphlearning.rocksdb;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
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
 * will read from the dataset-test/nodes.db and dataset-test/edges.db
 */
public class RocksDBReader {
    private String nodeDbPath;
    private String edgeDbPath;

    private RocksDB nodeDb;
    private RocksDB edgeDb;

    /** RocksDBReader with only dataset root. */
    public RocksDBReader(String dbPath) {
        this.nodeDbPath = dbPath + "/nodes.db";
        this.edgeDbPath = dbPath + "/edges.db";
        this.open();
    }

    /**
     * RocksDBReader constructor with costomized node and edges.db path.
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
     * RocksDBReader constructor with default node and edges.db path.
     *
     * <p>default nodes.db path: dataset-test/nodes.db
     *
     * <p>default edges.db path: dataset-test/edges.db
     */
    public RocksDBReader() {
        this.nodeDbPath = "dataset-test/nodes.db";
        this.edgeDbPath = "dataset-test/edges.db";
        this.open();
    }

    private void open() {
        try {
            nodeDb = RocksDB.openReadOnly(nodeDbPath);

            Options options2 = new Options();
            options2.optimizeForPointLookup(0);
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            edgeDb = RocksDB.openReadOnly(options2, edgeDbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /**
     * Query the node's K neighbors ID from edges.db.
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
     * Query the node's K neighbors ID from edges.db.
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
