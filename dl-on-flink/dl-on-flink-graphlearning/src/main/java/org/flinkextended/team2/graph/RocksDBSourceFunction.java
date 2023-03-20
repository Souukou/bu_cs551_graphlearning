package org.flinkextended.team2.graph;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Generate a stream data of incoming graph changes, i.e. new nodes from RocksDB DataStream format
 * Tuple5<Integer, Short, Integer, byte[], String>(NodeID, Mask, Label, Embedding, "Neighbors with
 * comma seperated")
 */
public class RocksDBSourceFunction
        implements SourceFunction<Tuple5<Integer, Short, Integer, byte[], String>> {

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
    public void run(SourceContext<Tuple5<Integer, Short, Integer, byte[], String>> ctx)
            throws Exception {
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);

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
        RocksDB nodesDB = RocksDB.open(options, nodesPath);
        RocksDB edgesDB = RocksDB.open(options, edgesPath);
        RocksDB neighborsDB = RocksDB.open(options, neighborsPath);
        //        try(RocksDB rocksDB = RocksDB.open(options, dbPath))
        //        {
        while (isRunning) {
            try (RocksIterator iterator = nodesDB.newIterator()) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    int key = Integer.parseInt(new String(iterator.key(), StandardCharsets.UTF_8));
                    byte[] value = iterator.value();
                    short mask = ByteBuffer.wrap(Arrays.copyOfRange(value, 0, 2)).getShort();
                    int label = ByteBuffer.wrap(Arrays.copyOfRange(value, 2, 6)).getInt();
                    byte[] embedding = Arrays.copyOfRange(value, 6, value.length + 1);
                    System.out.println("Inside RocksDB Iter");
                    String BracketNeighbors =
                            NeighborReader.find_neighbors(key, neighborsDB, edgesDB).toString();
                    String neighbors = BracketNeighbors.substring(1, BracketNeighbors.length() - 1);
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(new Tuple5<>(key, mask, label, embedding, neighbors));
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
