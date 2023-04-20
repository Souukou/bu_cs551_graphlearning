package graphlearning.rocksdb;

import org.apache.flink.api.java.tuple.Tuple3;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** NodeReader Interface. Provide a standalone method findFeatures. */
public interface NodeReader {
    /**
     * findNeighbor(Integer nodeId, RocksDB dbEdges).
     *
     * <p>NodeReader provides a standalone function that reads the given nodeId's label and features
     * attribute. It does not handle db connections and needs to pass a RocksDB handle into it.
     *
     * @param nodeId query node's id
     * @param db RocksDB instance
     * @return Tuple3&lt;Integer: nodeID, Integer: label, String: embedding&gt;
     */
    default Tuple3<Integer, Integer, String> findFeatures(Integer nodeId, RocksDB db) {
        try {
            byte[] keyByte = Integer.toString(nodeId).getBytes();
            byte[] value = db.get(keyByte);

            if (value == null) {
                // System.out.println("Value not found for key: " + new String(keyByte));
                // throw new RuntimeException("Value not found for key: " + new
                // String(keyByte));
                return null;
            }
            String[] valueStringArr = (new String(value)).split("\\|", 2);
            if (valueStringArr.length != 2) {
                // System.out.println("RocksDB value format error for key: " + new String(keyByte));
                throw new RuntimeException(
                        "RocksDB value format error for key: " + new String(keyByte));
            }
            int label = Integer.parseInt(valueStringArr[0]);
            String embeddingString = valueStringArr[1];
            Tuple3<Integer, Integer, String> entry = new Tuple3<>(nodeId, label, embeddingString);
            return entry;
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
    }
}
