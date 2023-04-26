package graphlearning;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import graphlearning.kafka.KafkaEventDeserializer;
import graphlearning.kafka.MapEventToEdge;
import graphlearning.kafka.protos.Event;
import graphlearning.maps.FlatMapNodeToComputationGraph;
import graphlearning.maps.MapComputationGraphToRow;
import graphlearning.sampling.Sampler;
import graphlearning.types.Edge;
import graphlearning.types.NodeComputationGraph;
import graphlearning.window.AggregateToList;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

class InputStream {
    DataStream<Row> getStream(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("prop.config"));
        } catch (IOException ex) {
            System.out.println("Error Reading Properties");
        }
        properties.setProperty("bootstrap.servers", "rise.bu.edu:9092");
        properties.setProperty("topic.id", "test");
        Integer maxNumNeighbors = Integer.parseInt(properties.getProperty("neighbors.num")),
                maxTrainingSamples = Integer.parseInt(properties.getProperty("reservoir.size")),
                depthOfCompGraph = Integer.parseInt(properties.getProperty("compgraph.depth"));
        int windowSize = 10;

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
        DataStream<Edge> edgeStream = kafkaStream.map(new MapEventToEdge());

        DataStream<List<Edge>> windowed =
                edgeStream.countWindowAll(windowSize).aggregate(new AggregateToList());

        DataStream<List<Integer>> sampledNodes =
                windowed.map(
                        new Sampler(
                                maxTrainingSamples,
                                "/opt/src/main/java/graphlearning/sampling/pretrained_nodes.json"));

        DataStream<NodeComputationGraph> compGraphs =
                sampledNodes.flatMap(
                        new FlatMapNodeToComputationGraph(maxNumNeighbors, depthOfCompGraph));

        DataStream<Row> rows = compGraphs.map(new MapComputationGraphToRow());

        return rows;
    }
}
