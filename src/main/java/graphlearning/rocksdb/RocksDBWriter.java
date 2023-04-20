package graphlearning.rocksdb;

import org.apache.flink.api.java.tuple.Tuple3;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * RocksDB writer interface to insert nodes and edges to RocksDB. By default, it will read/write
 * from the dataset-test/node.db, dataset-test/edge.db and dataset-test/neighbor.db
 */
public class RocksDBWriter implements NodeReader, NeighborReader {
    private String nodeDbPath;
    private String edgeDbPath;
    private String neighborPath;

    private RocksDB nodeDb;
    private RocksDB edgeDb;
    private RocksDB neighborDb;

    /**
     * RocksDBReader constructor with costomized node and edge db path.
     *
     * @param nodeDbPath
     * @param edgeDbPath
     */
    public RocksDBWriter(String nodeDbPath, String edgeDbPath, String neighborPath) {
        this.nodeDbPath = nodeDbPath;
        this.edgeDbPath = edgeDbPath;
        this.neighborPath = neighborPath;
        this.open();
    }

    /**
     * RocksDBReader constructor with default node and edge db path.
     *
     * <p>default node db path: dataset-test/node.db
     *
     * <p>default edge db path: dataset-test/edge.db
     */
    public RocksDBWriter() {
        this.nodeDbPath = "dataset-test/node.db";
        this.edgeDbPath = "dataset-test/edge.db";
        this.neighborPath = "dataset-test/neighbor.db";
        this.open();
    }

    static void createDirIfNotExists(String dbPath) {
        Path dbDirectory = Paths.get(dbPath).getParent();
        // System.out.println("dbDirectory: " + dbDirectory);
        // Create the directory if it doesn't exist
        if (!Files.exists(dbDirectory)) {
            try {
                Files.createDirectories(dbDirectory);
                System.out.println("Created directory for RocksDB: " + dbDirectory);
            } catch (IOException e) {
                System.err.println("Error creating directory for RocksDB: " + e.getMessage());
                e.printStackTrace();
                return;
            }
        }
    }

    private void open() {
        try {
            createDirIfNotExists(nodeDbPath);
            createDirIfNotExists(edgeDbPath);
            createDirIfNotExists(neighborPath);
            Options options = new Options();
            options.setCreateIfMissing(true);
            nodeDb = RocksDB.open(options, nodeDbPath);
            edgeDb = RocksDB.open(options, edgeDbPath);
            neighborDb = RocksDB.open(options, neighborPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /**
     * insertNode(Integer nodeId, String embedding). Insert a node into the node db. If the node
     * exist, do nothing.
     *
     * @param nodeId query node's id
     * @param feature query node's embedding string, will be convert to byte string to insert into
     *     rocksdb
     */
    public void insertNode(Integer nodeId, String feature) {
        insertNode(nodeId, feature.getBytes());
    }

    /**
     * insertNode(Integer nodeId, Integer label, Byte[] feature). Insert a node into the node db. If
     * the node exist, do nothing.
     *
     * @param nodeId query node's id
     * @param label query node's label
     * @param feature query node's embedding bytes
     */
    public void insertNode(Integer nodeId, Integer label, byte[] feature) {
        byte[] byteLabel = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(label).array();
        byte[] labelFeatureByte = new byte[feature.length + 4];
        System.arraycopy(byteLabel, 0, labelFeatureByte, 0, 4);
        System.arraycopy(feature, 0, labelFeatureByte, 4, feature.length);
        insertNode(nodeId, labelFeatureByte);
    }

    /**
     * insertNode(Integer nodeId, Byte[] feature). Insert a node into the node db. If the node
     * exist, do nothing.
     *
     * @param nodeId query node's id
     * @param feature query node's embedding bytes
     */
    public void insertNode(Integer nodeId, byte[] feature) {
        Tuple3<Integer, Integer, String> nodeFeatures = findFeatures(nodeId, nodeDb);

        // if this node feature is not null, the node already exist, return directly.
        if (nodeFeatures != null) {
            return;
        }

        try {
            byte[] keyByte = Integer.toString(nodeId).getBytes();
            byte[] valueByte = feature;
            nodeDb.put(keyByte, valueByte);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /**
     * insertEdge(Integer srcId, Integer dstId). Insert an edge into the edge db, also update the
     * number of neighbors in neighbor db. If the edge exist, do nothing.
     *
     * @param srcId query edge's source node id
     * @param dstId query edge's destination node id
     */
    public void insertEdge(Integer srcId, Integer dstId) {
        ArrayList<Integer> neighbors = findNeighbor(srcId, edgeDb);
        // if the edge already exist, return directly.
        if (neighbors != null && neighbors.contains(dstId)) {
            return;
        }
        // find the number of neighbors of srcId
        int neighborNum = neighbors == null ? 0 : neighbors.size();
        // insert the edge into the edge db
        try {
            byte[] keyByte = (String.format("%d|%d", srcId, neighborNum)).getBytes();
            byte[] valueByte = Integer.toString(dstId).getBytes();
            edgeDb.put(keyByte, valueByte);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        // update the number of edge in the neighbor db
        try {
            byte[] keyByte = Integer.toString(srcId).getBytes();
            byte[] valueByte = Integer.toString(neighborNum + 1).getBytes();
            neighborDb.put(keyByte, valueByte);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void finalize() {
        nodeDb.close();
        edgeDb.close();
        neighborDb.close();
    }
}
