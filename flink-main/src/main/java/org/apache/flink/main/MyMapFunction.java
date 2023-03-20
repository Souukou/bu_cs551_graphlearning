package org.apache.flink.main;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

public class MyMapFunction implements MapFunction<WikipediaEditEvent, GraphChange> {
    @Override
    public GraphChange map(WikipediaEditEvent e) {
        return new GraphChange(Tuple2.of(e.getUser(), e.getByteDiff()));
    }
}
