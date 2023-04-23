package graphlearning.sampling;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ReservoirTest. */
class ReservoirTest {
    private Reservoir reservoir;

    @BeforeEach
    void setUp() {
        reservoir = new Reservoir(10);
    }

    @Test
    void updateOneTime() {
        reservoir.update(1);
        List<Integer> reservoirList = reservoir.getReservoir();
        assertEquals(reservoirList.size(), 1);
        assertEquals(reservoirList.get(0), 1);
    }

    @Test
    void updateMultipleTimes() {
        int size = reservoir.getReservoir().size();
        for (int i = 0; i < 2 * size; i++) {
            reservoir.update(i);
        }
        assertEquals(reservoir.getReservoir().size(), size);
    }

    @Test
    void getSize() {
        int size = reservoir.getSize();
        reservoir.update(1);
        reservoir.update(2);
        assertEquals(reservoir.getSize(), size);
    }

    @Test
    void getReservoir() {
        reservoir.update(1);
        reservoir.update(2);
        List<Integer> list = reservoir.getReservoir();
        assertEquals(list.get(0), 1);
        assertEquals(list.get(1), 2);
        assertEquals(list.size(), 2);
    }

    @Test
    void sample() {
        for (int i = 0; i < 2 * reservoir.getSize(); i++) {
            reservoir.update(i);
        }
        List<Integer> list = reservoir.getReservoir();
        List<Integer> samples = reservoir.sample(5);
        assertEquals(samples.size(), 5);
        for (int sample : samples) {
            assertTrue(list.contains(sample));
        }
    }

    @Test
    void sampleMoreThanTimestamp() {
        reservoir.update(1);
        List<Integer> samples = reservoir.sample(2);
        assertEquals(samples, Arrays.asList(1));
    }

    @Test
    void sampleMoreThanSize() {
        for (int i = 0; i < 2 * reservoir.getSize(); i++) {
            reservoir.update(i);
        }
        List<Integer> samples = reservoir.sample(reservoir.getSize() + 2);
        assertEquals(samples.size(), reservoir.getSize());
    }
}
