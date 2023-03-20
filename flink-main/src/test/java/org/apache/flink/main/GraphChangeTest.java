package org.apache.flink.main;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GraphChangeTest {
    private GraphChange graphChange;

    @BeforeEach
    void setUp() {
        graphChange = new GraphChange(Tuple2.of("Iasonas", 42));
    }
    @Test
    void testToString() {
        assertEquals(graphChange.toString(), "(Iasonas, 42)");
    }

    @Test
    void testGetter() {
        assertEquals(graphChange.getInputGraphChange(), Tuple2.of("Iasonas", 42));
    }
}