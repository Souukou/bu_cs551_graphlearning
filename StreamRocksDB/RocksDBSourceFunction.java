import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.rocksdb.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.nio.charset.StandardCharsets;


/**
 * Generate a stream data of incoming graph changes, i.e. new edges from RocksDB
 * DataStream format Tuple2<Integer, Integer>(start node, end node)
 * */


public class RocksDBSourceFunction implements SourceFunction<Tuple2<Integer, Integer>> {

    private volatile boolean isRunning = true;
    private final String dbPath;

    public RocksDBSourceFunction(String dbPath) {
        this.dbPath = dbPath;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception
    {
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
        try(RocksDB rocksDB = RocksDB.open(options, dbPath))
        {
            while(isRunning)
            {
                try(RocksIterator iterator = rocksDB.newIterator())
                {
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                    {
                        String[] keyArray = new String(iterator.key(), StandardCharsets.UTF_8).split("\\|");
//                        String edge = new String(iterator.key(), StandardCharsets.UTF_8);
                        int src = Integer.parseInt(keyArray[0]);
                        int end = Integer.parseInt(keyArray[1]);
                        synchronized (ctx.getCheckpointLock())
                        {
                            ctx.collect(new Tuple2<>(src, end));
                        }
                    }
                }
                // wait for 1 second before reading next coming edge
                Thread.sleep(1000);
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;

    }
}
