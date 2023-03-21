package org.flinkextended.team2.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapToRow
        implements MapFunction<Tuple5<Integer, Short, Integer, List<Byte>, String>, Row> {
    private RocksDB db;

    public MapToRow(String path) throws RocksDBException {
        RocksDB.loadLibrary();
        Options options = new Options();
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

        this.db = RocksDB.openReadOnly(options, path);
    }
    /*
       Input: Tuple5
           -f0: nodeId (Integer)
           -f1: Mask (Short)
           -f2: Label (Integer)
           -f3: Embedding/Feature vector (byte[])
           -f4: neighbors of nodeId, e.g. "2,5,9" (String)
    */
    public Row map(Tuple5<Integer, Short, Integer, List<Byte>, String> tuple)
            throws RocksDBException {
        // convert byte[] to List<Byte>
        List<Byte> nodeEmbedding = tuple.f3;
        // System.out.println(nodeEmbedding);

        String neighbors = tuple.f4;

        List<Integer> neighborList =
                Stream.of(neighbors.split("-"))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());

        List<List<Byte>> embeddings = new ArrayList<>();
        embeddings.add(nodeEmbedding);

        // query database to find the embedding of the neighbors
        for (Integer neighbor : neighborList) {
            // import NodeReader!
            Tuple3<Integer, Integer, List<Byte>> entry = NodeReader.findFeatures(neighbor, db);
            List<Byte> neighborEmbedding = entry.f2;

            embeddings.add(neighborEmbedding);
        }
        // System.out.println(embeddings);

        // flatmap embeddings
        final List<Byte> flatEmbeddings =
                embeddings.stream().flatMap(l -> l.stream()).collect(Collectors.toList());

        // System.out.println(flatEmbeddings);
        Row row = new Row(4);
        String[] names = {"src", "label", "nbr", "embed"};
        row.setField(names[0], tuple.f0);
        row.setField(names[1], tuple.f2);
        row.setField(names[2], tuple.f4);
        row.setField(names[3], flatEmbeddings);
        return row;
    }
}
