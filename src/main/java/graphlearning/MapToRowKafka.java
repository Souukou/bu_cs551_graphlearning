package graphlearning;

import graphlearning.protos.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapToRow
        implements MapFunction<Event>, Row> {

    private final String db_path;

    public MapToRow(String path) {
        this.db_path = path;
    }
    /*
     * Input: Event
     * - timestamp: Current TimeStamp
     * - source: (integer) source node index
     * - target: (integer) target node index
     * - source_label: (integer) label of source node
     * - target_label: (integer) label of target node
     * - source_data_hex: (string) Byte String encoding of source embedding
     * - target_data_hex: (string) Byte String encoding of target node embedding
    */
    public Row map(Event event)
            throws RocksDBException {
        throw new NotImplementedException("This Function is yet to be implemeneted");

//         byte[] nodeEmbedding = ArrayUtils.toPrimitive(tuple.f3.toArray(new Byte[tuple.f3.size()]));
//         int length = nodeEmbedding.length;
//         String nodeEmbeddingChar = Hex.encodeHexString(nodeEmbedding);
//         RocksDB.loadLibrary();
//         Options options = new Options();
//         LRUCache cache = new LRUCache(2L * 1024 * 1024 * 1024);
//         LRUCache cacheCompressed = new LRUCache(500 * 1024 * 1024);
//         options.setTableFormatConfig(
//                 new BlockBasedTableConfig()
//                         .setFilter(new BloomFilter(10, false))
//                         .setBlockCache(cache)
//                         .setBlockCacheCompressed(cacheCompressed));
//         options.setMaxOpenFiles(300000);
//         options.setWriteBufferSize(67108864);
//         options.setMaxWriteBufferNumber(3);
//         options.setTargetFileSizeBase(67108864);
// 
//         RocksDB db = RocksDB.openReadOnly(options, this.db_path);
// 
//         String neighbors = tuple.f4;
// 
//         List<Integer> neighborList =
//                 Stream.of(neighbors.split("-"))
//                         .map(String::trim)
//                         .map(Integer::parseInt)
//                         .collect(Collectors.toList());
// 
//         List<String> embeddings = new ArrayList<>();
//         embeddings.add(nodeEmbeddingChar);
// 
//         // query database to find the embedding of the neighbors
//         for (Integer neighbor : neighborList) {
//             // import NodeReader!
//             Tuple3<Integer, Integer, List<Byte>> entry = NodeReader.findFeatures(neighbor, db);
//             List<Byte> neighborEmbedding = entry.f2;
//             Byte[] neighborEmbeddingB =
//                     neighborEmbedding.toArray(new Byte[neighborEmbedding.size()]);
//             byte[] neighborEmbeddingb = ArrayUtils.toPrimitive(neighborEmbeddingB);
//             embeddings.add(Hex.encodeHexString(neighborEmbeddingb));
//         }
// 
//         // flatmap embeddings
//         // final List<Byte> flatEmbeddings =
//         //        embeddings.stream().flatMap(l -> l.stream()).collect(Collectors.toList());
// 
//         //        Row row = Row.withPositions(4);
//         //        row.setField(0, tuple.f0);
//         //        row.setField(1, tuple.f2);
//         //        row.setField(2, tuple.f4);
//         //        row.setField(3, flatEmbeddings);
//         //        return row;
//         return Row.of(tuple.f0, tuple.f2, tuple.f4, String.join("", embeddings));
        return new Row();
    }
}
