package graphlearning.rocksdb;

import graphlearning.sampling.Reservoir;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** NeighborReader interface. Provide a standalone method findNeighbor. */
public class NeighborReader {
    /**
     * findNeighbor(Integer nodeId, RocksDB dbEdges, Integer maxNumOfNeighbors).
     *
     * <p>NeighborReader provides a standalone function that reads all the neighbor node ids from
     * RocksDB. It does not handle db connections and needs to pass a RocksDB handle into it.
     *
     * @param nodeId query node's id
     * @param dbEdges RocksDB instance
     * @param maxNumOfNeighbors maximum number of neighbors to return. If -1, return all neighbors.
     * @return ArrayList&lt;Integer&gt; of neighbor node ids
     */
    public static ArrayList<Integer> findNeighbor(
            Integer nodeId, RocksDB dbEdges, Integer maxNumOfNeighbors) {
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

        if (maxNumOfNeighbors != -1 && neighborsList.size() > maxNumOfNeighbors) {
            Collections.shuffle(neighborsList);
            List<Integer> sublist = neighborsList.subList(0, maxNumOfNeighbors);
            neighborsList = new ArrayList<Integer>(sublist);
        }
        return neighborsList;
    }

    /**
     * findNeighborReservoir(Integer nodeId, RocksDB dbEdges). Using Reservoir sampling to find the
     * neighbor nodes.
     *
     * <p>NeighborReader provides a standalone function that reads all the neighbor node ids from
     * RocksDB. It does not handle db connections and needs to pass a RocksDB handle into it.
     *
     * @param nodeId query node's id
     * @param dbEdges RocksDB instance
     * @param maxNumOfNeighbors maximum number of neighbors to be returned.
     * @return ArrayList&lt;Integer&gt; of neighbor node ids
     */
    public static ArrayList<Integer> findNeighborReservoir(
            Integer nodeId, RocksDB dbEdges, Integer maxNumOfNeighbors) {
        ArrayList<Integer> neighborsList = new ArrayList<Integer>();
        if (maxNumOfNeighbors == -1) {
            System.out.println("Max Number of Neighbors: " + maxNumOfNeighbors);
        }
        Reservoir reservoir = new Reservoir(maxNumOfNeighbors);

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
            reservoir.update(currentNeighbor);
        }
        neighborsList = new ArrayList<Integer>(reservoir.sample(maxNumOfNeighbors));
        return neighborsList;
    }
}
