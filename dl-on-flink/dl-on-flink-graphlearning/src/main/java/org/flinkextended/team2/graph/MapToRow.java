package org.flinkextended.team2.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapToRow implements MapFunction<Tuple5<Integer, Short, Integer, byte[], String>, Row> {
    private RocksDB db;

    public MapToRow(RocksDB db) {
        this.db = db;
    }
    /*
        Input: Tuple5
            -f0: nodeId (Integer)
            -f1: Mask (Short)
            -f2: Label (Integer)
            -f3: Embedding/Feature vector (byte[])
            -f4: neighbors of nodeId, e.g. "2,5,9" (String)
     */
    @Override
    public Tuple5<Integer, Short, Integer, byte[], String> map(Tuple5<Integer, Short, Integer, byte[], String> tuple) {
        // convert byte[] to List<Byte>
        List<Byte> nodeEmbedding = tuple.f3;
        // System.out.println(nodeEmbedding);

        String neighbors = tuple.f4;

        List<Integer> neighborList = Stream.of(neighbors.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        List<List<Byte>> embeddings = new ArrayList<>();
        embeddings.add(nodeEmbedding);

        //query database to find the embedding of the neighbors
        for (Integer neighbor : neighborList) {
            // import NodeReader!
            Tuple3<Integer, Integer, List<Byte>> entry = NodeReader.findFeatures(neighbor, db);
            List<Byte> neighborEmbedding = entry.f2;

            embeddings.add(neighborEmbedding);
        }
        // System.out.println(embeddings);

        //flatmap embeddings
        final List<Byte> flatEmbeddings = embeddings.stream()
                .flatMap(l -> l.stream())
                .collect(Collectors.toList());

        // System.out.println(flatEmbeddings);
        return Row.of(tuple.f0, tuple.f4, tuple.f2, flatEmbeddings);
    }

}
