package org.flinkextended.team2.graph;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

class InputStream {
    String nodesPath = "dataset-test/nodes.db";
    String edgesPath = "dataset-test/edges.db";
    String neighborPath = "dataset-test/neighbor.db";

<<<<<<< HEAD
    DataStream<Row> getStream(RocksDB db) {
        DataStream<Tuple5<Integer, Short, Integer, byte[], String>> inputStream = new RocksDBSourceFunction(
                nodesPath, edgesPath, neighborPath
        );
        DataStream<Row> rows = inputStream.map(new MapToRow(db));
=======
    DataStream<Row> getStream(StreamExecutionEnvironment env) {
        DataStream<Tuple5<Integer, Short, Integer, byte[], String>> inputStream =
                env.addSource(new RocksDBSourceFunction(nodesPath, edgesPath, neighborPath));
        DataStream<Row> rows = inputStream.map(new MapToRow());
>>>>>>> 6f10f4d12cc2633a30e257edf2019c45ace473db
        return rows;
    }
}
