import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public interface NeighborReader {
    public static ArrayList<Integer> find_neighbors(Integer key, RocksDB dbNeighbors, RocksDB dbEdges) throws RocksDBException {
        ArrayList<Integer> neighborsList = new ArrayList<Integer>();
        RocksIterator iterEdges = dbEdges.newIterator();
        byte[] keyByte = Integer.toString(key).getBytes();
        byte[] firstEdgesKey = dbNeighbors.get(keyByte);

        iterEdges.seek(firstEdgesKey);
        for (iterEdges.seek(firstEdgesKey); iterEdges.isValid(); iterEdges.prev()) {
            String currentKey = new String(iterEdges.key());
            if (!currentKey.startsWith(key.toString())) {
                break;
            }
            String[] keyArray = new String(iterEdges.key(), StandardCharsets.UTF_8).split("\\|");
            Integer currentNeighbor = Integer.parseInt(keyArray[1]);
            neighborsList.add(currentNeighbor);
        }
        return neighborsList;
    }
}
