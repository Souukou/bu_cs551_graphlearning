package main;

import org.apache.flink.api.java.tuple.Tuple3;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.flink.api.java.tuple.Tuple5;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * RandomTenNodes(RocksDB nodesDb, RocksDB edgesDb, RocksDB neighborDb)
 * Input: Three RocksDB instances indicating nodesDb, edgesDb and neighborDb
 * Output: ArrayList<Tuple5> of size 10, each element is a Tuple5 which stores (NodeID, Mast, Label, Embedding, Neighbors)
 * in the format of <Integer, Short, Integer, byte[], String>
 * */


public interface RandomNodes {
    public static ArrayList<Tuple5<Integer, Short, Integer, byte[], String>> RandomTenNodes(RocksDB nodesDb,
                                                                                          RocksDB edgesDb,
                                                                                          RocksDB neighborDb) throws RocksDBException {
        ArrayList<Tuple5<Integer, Short, Integer, byte[], String>> NodesList = new ArrayList<>();
        Random random = new Random();
        for(int i=0; i<10; i++){
            int randomKey = random.nextInt(34);
            Tuple3 nodesEntry = NodeReader.findFeatures(randomKey, nodesDb);
            short mask = (short) nodesEntry.f0;
            int label = (int) nodesEntry.f1;
            byte[] embedding = (byte[]) nodesEntry.f2;
            String BracketNeighbors = NeighborReader.find_neighbors(randomKey, neighborDb, edgesDb).toString();
            String neighbors = BracketNeighbors.substring(1, BracketNeighbors.length() - 1);
            Tuple5<Integer, Short, Integer, byte[], String> randomNode = new Tuple5<>(randomKey, mask, label, embedding, neighbors);
            NodesList.add(randomNode);
        }
        return NodesList;
    }
}


