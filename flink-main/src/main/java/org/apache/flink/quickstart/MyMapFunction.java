package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

public class MyMapFunction implements MapFunction<WikipediaEditEvent, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(WikipediaEditEvent e) {
        return Tuple2.of(e.getUser(), e.getByteDiff());
    }
}
