package graphlearning.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;

/** NeighborReader interface. Provide a standalone method findNeighbor. */
public interface NeighborReader {
    /**
     * findNeighbor(Integer nodeId, RocksDB dbEdges).
     *
     * <p>NeighborReader provides a standalone function that reads all the neighbor node ids from
     * RocksDB. It does not handle db connections and needs to pass a RocksDB handle into it.
     *
     * @param nodeId query node's id
     * @param dbEdges RocksDB instance
     * @return ArrayList&lt;Integer&gt; of neighbor node ids
     */
    default ArrayList<Integer> findNeighbor(Integer nodeId, RocksDB dbEdges) {
        ArrayList<Integer> neighborsList = new ArrayList<Integer>();

        RocksIterator iterEdges = dbEdges.newIterator();

        // iterate from start point to find all the neighbors
        byte[] firstEdgesKey = (String.format("%d|0", nodeId)).getBytes();

        for (iterEdges.seek(firstEdgesKey); iterEdges.isValid(); iterEdges.next()) {
            String[] keyArray = new String(iterEdges.key()).split("\\|");
            // if the key is not the same as the node id, then all the neighbors has already
            // been found
            if (!keyArray[0].equals(nodeId.toString())) {
                break;
            }
            Integer currentNeighbor = Integer.parseInt(new String(iterEdges.value()));
            neighborsList.add(currentNeighbor);
        }
        return neighborsList;
    }
}
