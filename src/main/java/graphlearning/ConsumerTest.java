package org.kafka.consumer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.protobuf.Timestamp;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.kafka.consumer.EventProto.Event;

import java.util.ArrayList;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumerTest {
    public static void main(String[] args) throws java.lang.Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "rise.bu.edu:9092");
        props.setProperty("topic.id", "test");

        KafkaSource<Event> kafkaSource =
                KafkaSource.<Event>builder()
                        .setBootstrapServers(props.getProperty("bootstrap.servers"))
                        .setTopics(props.getProperty("topic.id"))
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new EventDeserializationSchema())
                        .build();

        DataStream<Event> kafkaStream =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaStream.print();

        DataStream<
                        Tuple7<
                                Integer,
                                Integer,
                                Integer,
                                Integer,
                                ArrayList<Byte>,
                                ArrayList<Byte>,
                                Timestamp>>
                tranStream =
                        kafkaStream.map(
                                (event) -> {
                                    byte[] sourceDataBytes = decodeHex(event.getSourceDataHex());
                                    byte[] targetDataBytes = decodeHex(event.getTargetDataHex());

                                    ArrayList<Byte> sourceDataList =
                                            byteArrayToArrayList(sourceDataBytes);
                                    ArrayList<Byte> targetDataList =
                                            byteArrayToArrayList(targetDataBytes);

                                    Tuple7<
                                                    Integer,
                                                    Integer,
                                                    Integer,
                                                    Integer,
                                                    ArrayList<Byte>,
                                                    ArrayList<Byte>,
                                                    Timestamp>
                                            transEvnet =
                                                    new Tuple7<>(
                                                            event.getSource(),
                                                            event.getTarget(),
                                                            event.getSourceLabel(),
                                                            event.getTargetLabel(),
                                                            sourceDataList,
                                                            targetDataList,
                                                            //
                                                            // Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos())
                                                            event.getTimestamp());
                                    return transEvnet;
                                });

        tranStream.print();
        env.execute("Event Source");
    }

    private static byte[] decodeHex(String hexString) throws DecoderException {
        return Hex.decodeHex(hexString.toCharArray());
    }

    private static ArrayList<Byte> byteArrayToArrayList(byte[] byteArray) {
        return IntStream.range(0, byteArray.length)
                .mapToObj(i -> byteArray[i])
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
