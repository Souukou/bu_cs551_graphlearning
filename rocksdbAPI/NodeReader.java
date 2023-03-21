package main;

import org.rocksdb.*;
import org.apache.flink.api.java.tuple.Tuple3;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 *findFeatures(Integer nodeID, RocksDB db)
 * input: query node's id; a RocksDB instance
 * output: flink Tuple3(Integer: mask, Integer: label, byte[]: embedding)
 **/

public interface NodeReader {
    public static Tuple3 findFeatures(Integer nodeID, RocksDB db) throws RocksDBException {
        byte[] keyByte = Integer.toString(nodeID).getBytes();
        byte[] value = db.get(keyByte);

        if (value == null) {
            System.out.println("Value not found for key: " + new String(keyByte));
        }
        int keyInt = nodeID;
        int mask = ByteBuffer.wrap(Arrays.copyOfRange(value, 0, 7)).getInt();
        int label = ByteBuffer.wrap(Arrays.copyOfRange(value, 8, 15)).getInt();
        byte[] embedding = Arrays.copyOfRange(value, 16, value.length);
        List<Byte> embeddingList = Arrays.asList(Arrays.stream(embedding)
                .boxed()
                .toArray(Byte[]::new));
        Tuple3<Integer, Integer, List<Byte>> entry = new Tuple3<>(mask, label, embedding);
        return entry;
    }
}
