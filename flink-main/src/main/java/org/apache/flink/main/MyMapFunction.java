package org.apache.flink.main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

public class MyMapFunction implements MapFunction<WikipediaEditEvent, GraphChange> {
    @Override
    public GraphChange map(WikipediaEditEvent e) {
        byte[] byteArray = "This is a byte string".getBytes();
        return new GraphChange(Tuple5.of(42, Short.valueOf("1"), 17, byteArray, "3-4-5"));
    }
}
