package org.apache.flink.quickstart;

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
        Tuple2<String, Integer> value = Tuple2.of("Iasonas", 42);
        List<Tuple2<String, Integer>> acc = new ArrayList<>();

        List<Tuple2<String, Integer>> result = myAggregateFunction.add(value, acc);

        assertEquals(result, Arrays.asList(value));
    }

    @Test
    void testGetResult() {
        Tuple2<String, Integer> value = Tuple2.of("Iasonas", 42);
        List<Tuple2<String, Integer>> acc = Arrays.asList(value);

        List<Tuple2<String, Integer>> result = myAggregateFunction.getResult(acc);

        assertEquals(result, Arrays.asList(value));
    }

    @Test
    void testMerge() {
        Tuple2<String, Integer> val1 = Tuple2.of("Iasonas", 42);
        Tuple2<String, Integer> val2 = Tuple2.of("Adrish", 17);
        Tuple2<String, Integer> val3 = Tuple2.of("Aoming", 23);

        List<Tuple2<String, Integer>> acc1 = new ArrayList<>();
        acc1.add(val1);
        acc1.add(val2);
        List<Tuple2<String, Integer>> acc2 = Arrays.asList(val3);

        List<Tuple2<String, Integer>> result = myAggregateFunction.merge(acc1, acc2);

        assertEquals(result, Arrays.asList(val1, val2, val3));

    }
}