package graphlearning.kafka;

import org.apache.flink.api.common.functions.MapFunction;

import graphlearning.kafka.protos.Event;
import graphlearning.types.Edge;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MapEventToEdge implements MapFunction<Event, Edge> {

    @Override
    public Edge map(Event event) throws Exception {
        byte[] sourceDataBytes = decodeHex(event.getSourceDataHex());
        byte[] targetDataBytes = decodeHex(event.getTargetDataHex());

        ArrayList<Byte> sourceDataList = byteArrayToArrayList(sourceDataBytes);
        ArrayList<Byte> targetDataList = byteArrayToArrayList(targetDataBytes);
        Edge edge =
                Edge.builder()
                        .sourceNode(event.getSource())
                        .targetNode(event.getTarget())
                        .sourceLabel(event.getSourceLabel())
                        .targetLabel(event.getTargetLabel())
                        .sourceEmbedding(sourceDataList)
                        .targetEmbedding(targetDataList)
                        .timestamp(event.getTimestamp().toString())
                        .build();
        return edge;
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
