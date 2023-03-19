package main;

import org.rocksdb.*;
import org.apache.flink.api.java.tuple.Tuple3;
import java.nio.ByteBuffer;
import java.util.Arrays;


public interface NodeReader {
    public static Tuple3 findFeatures(int key, RocksDB db) throws RocksDBException {
        byte[] keyByte = Integer.toString(key).getBytes();
        byte[] value = db.get(keyByte);

        if (value == null) {
            System.out.println("Value not found for key: " + new String(keyByte));
        }
        int keyInt = key;
        int mask = ByteBuffer.wrap(Arrays.copyOfRange(value, 0, 7)).getInt();
        int label = ByteBuffer.wrap(Arrays.copyOfRange(value, 8, 15)).getInt();
        byte[] embedding = Arrays.copyOfRange(value, 16, value.length);
        Tuple3<Integer, Integer, byte[]> entry = new Tuple3<>(mask, label, embedding);
        return entry;
    }
}
