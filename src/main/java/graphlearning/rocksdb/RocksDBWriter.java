package graphlearning.rocksdb;

import org.rocksdb.ComparatorOptions;
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
 * from the dataset-test/nodes.db, dataset-test/edges.db and dataset-test/neighbor.db
 */
public class RocksDBWriter {
    private String nodeDbPath;
    private String edgeDbPath;
    private String neighborPath;

    private RocksDB nodeDb;
    private RocksDB edgeDb;
    private RocksDB neighborDb;

    /** RocksDBReader with only dataset root. */
    public RocksDBWriter(String dbPath) {
        this.nodeDbPath = dbPath + "/nodes.db";
        this.edgeDbPath = dbPath + "/edges.db";
        this.neighborPath = dbPath + "/neighbor.db";
        this.open();
    }

    /**
     * RocksDBReader constructor with costomized node and edges.db path.
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
     * RocksDBReader constructor with default node and edges.db path.
     *
     * <p>default nodes.db path: dataset-test/nodes.db
     *
     * <p>default edges.db path: dataset-test/edges.db
     */
    public RocksDBWriter() {
        this.nodeDbPath = "dataset-test/nodes.db";
        this.edgeDbPath = "dataset-test/edges.db";
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
            RocksDB.loadLibrary();
            Options options = new Options();
            options.setCreateIfMissing(true);
            // nodeDb = RocksDB.open(options, nodeDbPath);
            neighborDb = RocksDB.open(options, neighborPath);

            Options options2 = new Options();
            ComparatorOptions comparatorOptions = new ComparatorOptions();
            options2.setCreateIfMissing(true);
            options2.setComparator(new OrderByCountComparator(comparatorOptions));
            edgeDb = RocksDB.open(options2, edgeDbPath);

        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /**
     * insertNode(Integer nodeId, String embedding). Insert a node into the nodes.db. If the node
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
     * insertNode(Integer nodeId, Integer label, Byte[] feature). Insert a node into the nodes.db.
     * If the node exist, do nothing.
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
     * insertNode(Integer nodeId, Byte[] feature). Insert a node into the nodes.db. If the node
     * exist, do nothing.
     *
     * @param nodeId query node's id
     * @param feature query node's embedding bytes
     */
    public void insertNode(Integer nodeId, byte[] feature) {
        return;
        //        try {
        //            byte[] keyByte = Integer.toString(nodeId).getBytes();
        //            byte[] valueByte = feature;
        //            nodeDb.put(keyByte, valueByte);
        //        } catch (RocksDBException e) {
        //            e.printStackTrace();
        //        }
    }

    /**
     * insertEdge(Integer srcId, Integer dstId). Insert an edge into the edges.db, also update the
     * number of neighbors in neighbor db. If the edge exist, do nothing.
     *
     * @param srcId query edge's source node id
     * @param dstId query edge's destination node id
     */
    public void insertEdge(Integer srcId, Integer dstId) {
        ArrayList<Integer> neighbors = NeighborReader.findNeighbor(srcId, edgeDb, -1);
        // if the edge already exist, return directly.
        if (neighbors != null && neighbors.contains(dstId)) {
            return;
        }
        // find the number of neighbors of srcId
        int neighborNum = neighbors == null ? 0 : neighbors.size();
        // insert the edge into the edges.db
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

    public void flushNode() {
        //        try {
        //            FlushOptions flushOptions = new FlushOptions();
        //            flushOptions.setWaitForFlush(true);
        //            // flush then return
        //            nodeDb.flush(flushOptions);
        //        } catch (RocksDBException e) {
        //            e.printStackTrace();
        //            System.out.println("flush node db failed");
        //        }
    }

    public void finalize() {
        // nodeDb.close();
        edgeDb.close();
        neighborDb.close();
    }
}
