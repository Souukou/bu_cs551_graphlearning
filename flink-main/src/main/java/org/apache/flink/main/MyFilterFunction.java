package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;

public class MyFilterFunction implements FilterFunction<WikipediaEditEvent> {
    @Override
    public boolean filter(WikipediaEditEvent e) {
        return e.getByteDiff() > 0;
    }
}
