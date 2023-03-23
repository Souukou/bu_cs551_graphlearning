package org.flinkextended.team2.graph;

import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.commons.lang3.ArrayUtils;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * findFeatures(Integer nodeID, RocksDB db) input: query node's id; a RocksDB instance output: flink
 * Tuple3(Integer: mask, Integer: label, byte[]: embedding)
 */
public interface NodeReader {
    public static Tuple3 findFeatures(Integer nodeID, RocksDB db) throws RocksDBException {
        byte[] keyByte = Integer.toString(nodeID).getBytes();
        byte[] value = db.get(keyByte);

        if (value == null) {
            System.out.println("Value not found for key: " + new String(keyByte));
        }
        int keyInt = nodeID;
        int mask = ByteBuffer.wrap(Arrays.copyOfRange(value, 0, 2)).getShort();
        int label = ByteBuffer.wrap(Arrays.copyOfRange(value, 2, 6)).getInt();
        byte[] embedding = Arrays.copyOfRange(value, 6, value.length);
        List<Byte> embeddingList = Arrays.asList(ArrayUtils.toObject(embedding));
        Tuple3<Integer, Integer, List<Byte>> entry = new Tuple3<>(mask, label, embeddingList);
        return entry;
    }
}
