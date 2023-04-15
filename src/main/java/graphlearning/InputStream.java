package graphlearning;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import graphlearning.protos.Event;

import java.util.Properties;

class InputStream {
    String nodesPath = "dataset-test/nodes.db";
    //     String edgesPath = "dataset-test/edges.db";
    //     String neighborPath = "dataset-test/neighbor.db";
    //
    //     DataStream<Row> getStream(StreamExecutionEnvironment env) throws RocksDBException {
    //         RocksDBSourceFunction source =
    //                 new RocksDBSourceFunction(nodesPath, edgesPath, neighborPath);
    //         DataStream<Tuple5<Integer, Short, Integer, List<Byte>, String>> inputStream =
    //                 env.addSource(source);
    //         DataStream<Row> rows = inputStream.map(new MapToRow(nodesPath));
    //
    //         return rows;
    //     }
    DataStream<Row> getStream(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "rise.bu.edu:9092");
        properties.setProperty("topic.id", "test");

        // create a Kafka consumer
        KafkaSource<Event> source =
                KafkaSource.<Event>builder()
                        .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                        .setTopics(properties.getProperty("topic.id"))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new KafkaEventDeserializer())
                        .build();

        DataStream<Event> kafkaStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Row> rows = kafkaStream.map(new MapToRowKafka(nodesPath));
        return rows;
    }
}
