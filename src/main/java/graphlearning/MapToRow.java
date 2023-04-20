package graphlearning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import graphlearning.rocksdb.NodeReader;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** MapToRow. */
public class MapToRow
        implements MapFunction<Tuple5<Integer, Short, Integer, List<Byte>, String>, Row>,
                NodeReader {

    private final String dbPath;

    public MapToRow(String path) {
        this.dbPath = path;
    }

    /*
     * Input: Tuple5
     * -f0: nodeId (Integer)
     * -f1: Mask (Short)
     * -f2: Label (Integer)
     * -f3: Embedding/Feature vector (byte[])
     * -f4: neighbors of nodeId, e.g. "2,5,9" (String)
     */
    public Row map(Tuple5<Integer, Short, Integer, List<Byte>, String> tuple)
            throws RocksDBException {
        // convert byte[] to List<Byte>
        byte[] nodeEmbedding = ArrayUtils.toPrimitive(tuple.f3.toArray(new Byte[tuple.f3.size()]));
        int length = nodeEmbedding.length;
        String nodeEmbeddingChar = Hex.encodeHexString(nodeEmbedding);
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

        RocksDB db = RocksDB.openReadOnly(options, this.dbPath);

        String neighbors = tuple.f4;

        List<Integer> neighborList =
                Stream.of(neighbors.split("-"))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());

        List<String> embeddings = new ArrayList<>();
        embeddings.add(nodeEmbeddingChar);

        // query database to find the embedding of the neighbors
        for (Integer neighbor : neighborList) {
            // import NodeReader!
            Tuple3<Integer, Integer, String> entry = findFeatures(neighbor, db);
            embeddings.add(entry.f2);
        }

        // flatmap embeddings
        // final List<Byte> flatEmbeddings =
        // embeddings.stream().flatMap(l -> l.stream()).collect(Collectors.toList());

        // Row row = Row.withPositions(4);
        // row.setField(0, tuple.f0);
        // row.setField(1, tuple.f2);
        // row.setField(2, tuple.f4);
        // row.setField(3, flatEmbeddings);
        // return row;
        return Row.of(tuple.f0, tuple.f2, tuple.f4, String.join("", embeddings));
    }
}
