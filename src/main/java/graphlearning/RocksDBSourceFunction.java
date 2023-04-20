package graphlearning;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import graphlearning.rocksdb.NeighborReader;
import org.apache.commons.lang3.ArrayUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generate a stream data of incoming graph changes, i.e. new nodes from RocksDB DataStream format
 * Tuple5 < Integer, Short, Integer, byte[], String > (NodeID, Mask, Label, Embedding, "Neighbors
 * with comma seperated")
 */
public class RocksDBSourceFunction
        extends RichParallelSourceFunction<Tuple5<Integer, Short, Integer, List<Byte>, String>>
        implements NeighborReader {

    private volatile boolean isRunning = true;
    private final String nodesPath;

    private final String edgesPath;

    private final String neighborsPath;

    public RocksDBSourceFunction(String nodesPath, String edgesPath, String neighborsPath) {
        this.nodesPath = nodesPath;
        this.edgesPath = edgesPath;
        this.neighborsPath = neighborsPath;
    }

    @Override
    public void run(SourceContext<Tuple5<Integer, Short, Integer, List<Byte>, String>> ctx)
            throws RocksDBException // throws Exception
            {
        RocksDB.loadLibrary();
        Options options = new Options();

        // Fastest RocksDB settings
        LRUCache cache = new LRUCache(2L * 1024 * 1024 * 1024);
        LRUCache cacheCompressed = new LRUCache(500 * 1024 * 1024);
        options.setTableFormatConfig(
                new BlockBasedTableConfig()
                        .setFilter(new BloomFilter(10, false))
                        .setBlockCache(cache)
                        .setBlockCacheCompressed(cacheCompressed));
        options.setMaxOpenFiles(300000);
        options.setWriteBufferSize(67108864);
        options.setMaxWriteBufferNumber(3);
        options.setTargetFileSizeBase(67108864);

        try {
            RocksDB nodesDB = RocksDB.openReadOnly(options, nodesPath);
            RocksDB edgesDB = RocksDB.openReadOnly(options, edgesPath);
            RocksDB neighborsDB = RocksDB.openReadOnly(options, neighborsPath);

            RocksIterator iterator = nodesDB.newIterator();
            iterator.seekToFirst();
            while (isRunning && iterator.isValid()) {
                // System.out.println("Rocks Iter");
                int key = Integer.parseInt(new String(iterator.key(), StandardCharsets.UTF_8));
                byte[] value = iterator.value();
                short mask = ByteBuffer.wrap(Arrays.copyOfRange(value, 0, 2)).getShort();
                int label = ByteBuffer.wrap(Arrays.copyOfRange(value, 2, 6)).getInt();
                List<Byte> embedding =
                        Arrays.asList(
                                ArrayUtils.toObject(Arrays.copyOfRange(value, 6, value.length)));
                String neighbors =
                        findNeighbor(key, edgesDB).stream()
                                .map(String::valueOf)
                                .collect(Collectors.joining("-"));
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new Tuple5<>(key, mask, label, embedding, neighbors));
                }
                iterator.next();
            }
        } catch (RocksDBException e) {
            System.err.println("Error opening the RocksDB: " + e);
        }

        // try(RocksDB rocksDB = RocksDB.open(options, dbPath))
        // {

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
