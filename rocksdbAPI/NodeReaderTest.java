package main;
import org.rocksdb.*;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Test of NodeReader.findFeatures
 * */

public class NodeReaderTest implements NodeReader{
    public static void main(String[] args) throws RocksDBException {
        String dbPath = "dataset-test/nodes.db";
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

        RocksDB db = RocksDB.open(options, dbPath);
        Tuple3 tuple = NodeReader.findFeatures(0, db);
        System.out.println("Finish");

        if(db != null){
            db.close();
        }
    }
}

