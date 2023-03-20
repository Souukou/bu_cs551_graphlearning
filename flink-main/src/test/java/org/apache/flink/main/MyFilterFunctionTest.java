package org.apache.flink.quickstart;

import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.*;
import static org.junit.jupiter.api.Assertions.*;

class MyFilterFunctionTest {
    private WikipediaEditEvent editEvent;
    private MyFilterFunction myFilter;
    @BeforeEach
    void setup() {
        editEvent = mock(WikipediaEditEvent.class);
        myFilter = new MyFilterFunction();
    }

    @Test
    void testFilterWithByteDiff() {
        expect(editEvent.getByteDiff()).andReturn(42);
        replay(editEvent);
        assertTrue(myFilter.filter(editEvent));
        verify(editEvent);
    }
    @Test
    void testFilterWithoutByteDiff() {
        expect(editEvent.getByteDiff()).andReturn(0);
        replay(editEvent);
        assertFalse(myFilter.filter(editEvent));
        verify(editEvent);
    }
}