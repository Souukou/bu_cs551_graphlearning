package org.apache.flink.quickstart;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.*;
import static org.junit.jupiter.api.Assertions.*;

class MyMapFunctionTest {
    private WikipediaEditEvent editEvent;
    private MyMapFunction myMap;
    @BeforeEach
    void setUp() {
        editEvent = mock(WikipediaEditEvent.class);
        myMap = new MyMapFunction();
    }

    @Test
    void testMap() {
        expect(editEvent.getUser()).andReturn("Iasonas");
        expect(editEvent.getByteDiff()).andReturn(42);
        replay(editEvent);

        Tuple2 result = myMap.map(editEvent);

        assertEquals(result.f0, "Iasonas");
        assertEquals(result.f1, 42);
        verify(editEvent);
    }
}