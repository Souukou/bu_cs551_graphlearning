package org.apache.flink.main;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MyAggregateFunctionTest {
    private MyAggregateFunction myAggregateFunction;

    @BeforeEach
    void setUp() {
        myAggregateFunction = new MyAggregateFunction();
    }

    @Test
    void testCreateAccumulator() {
        assertTrue(myAggregateFunction.createAccumulator().isEmpty());
    }

    @Test
    void testAdd() {
        GraphChange value = new GraphChange(Tuple2.of("Iasonas", 42));
        List<GraphChange> acc = new ArrayList<>();

        List<GraphChange> result = myAggregateFunction.add(value, acc);

        assertEquals(result, Arrays.asList(value));
    }

    @Test
    void testGetResult() {
        GraphChange value = new GraphChange(Tuple2.of("Iasonas", 42));
        List<GraphChange> acc = Arrays.asList(value);

        List<GraphChange> result = myAggregateFunction.getResult(acc);

        assertEquals(result, Arrays.asList(value));
    }

    @Test
    void testMerge() {
        GraphChange val1 = new GraphChange(Tuple2.of("Iasonas", 42));
        GraphChange val2 = new GraphChange(Tuple2.of("Adrish", 17));
        GraphChange val3 = new GraphChange(Tuple2.of("Aoming", 23));

        List<GraphChange> acc1 = new ArrayList<>();
        acc1.add(val1);
        acc1.add(val2);
        List<GraphChange> acc2 = Arrays.asList(val3);

        List<GraphChange> result = myAggregateFunction.merge(acc1, acc2);

        assertEquals(result, Arrays.asList(val1, val2, val3));
    }
}