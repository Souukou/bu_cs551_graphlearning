package main;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple5;
import org.rocksdb.*;

import java.util.ArrayList;


/**
 * Test for RandomTenNodes function
 * */


public class RandomNodesTest {
    public static void main(String[] args) throws Exception {

        String nodesPath = "dataset-test/nodes.db";
        String edgesPath = "dataset-test/edges.db";
        String neighborPath = "dataset-test/neighbor.db";

        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);

        // Fastest RocksDB settings
        LRUCache cache = new LRUCache(2L * 1024 * 1024 * 1024);
        LRUCache cacheCompressed = new LRUCache(500 * 1024 * 1024);
        options.setTableFormatConfig(new BlockBasedTableConfig()
                .setFilter(new BloomFilter(10, false))
                .setBlockCache(cache)
                .setBlockCacheCompressed(cacheCompressed)
        );
        options.setMaxOpenFiles(300000);
        options.setWriteBufferSize(67108864);
        options.setMaxWriteBufferNumber(3);
        options.setTargetFileSizeBase(67108864);
        RocksDB nodesDB = RocksDB.open(options, nodesPath);
        RocksDB edgesDB = RocksDB.open(options, edgesPath);
        RocksDB neighborDB = RocksDB.open(options, neighborPath);

        ArrayList<Tuple5<Integer, Short, Integer, byte[], String>> NodesList = new ArrayList<>();
        for (int i=0;i<5;i++){
            NodesList = RandomNodes.RandomTenNodes(nodesDB, edgesDB, neighborDB);
        }


    }

}
