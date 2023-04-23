package graphlearning.helper;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RandomNumbersTest {

    @Test
    void randomNumbers() {
        List<Integer> nums = RandomNumbers.randomNumbers(1, 10, 5);
        assertEquals(nums.size(), 5);
        for (int num : nums) {
            assertTrue(num >= 1);
            assertTrue(num <= 10);
        }
    }
}
