package graphlearning.helper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/** Get a list of random numbers within (min, max). */
public class RandomNumbers {
    private static Random random = new Random();

    public static List<Integer> randomNumbers(int min, int max, int numOfSamples) {
        /** Samples numOfSamples integers in the range [min, max] without replacement */
        Set<Integer> generated = new HashSet<>();

        while (generated.size() < numOfSamples) {
            int randomNum = random.nextInt((max - min) + 1) + min;
            if (!generated.contains(randomNum)) {
                generated.add(randomNum);
            }
        }
        List<Integer> randomNums = new ArrayList<>();
        randomNums.addAll(generated);
        return randomNums;
    }
}
